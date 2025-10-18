package bigsort

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"slices"
	"sync"

	"github.com/garethgeorge/gosnapraid/internal/bigsort/buffers"
)

const DefaultMaxBufferSizeBytes = 256 * 1024 * 1024 // 256 MB

type BigSorter[T BigSortable] struct {
	// bufferFactory is used to create new buffers for storing sorted blocks
	bufferFactory buffers.BufferFactory

	// maxBlockSizeBytes is the maximum size of each block buffer before flushing to storage
	maxBlockSizeBytes int64

	// curBlockSizeBytes is the current size of the block being built
	curBlockSizeBytes int64
	curBlock          []T

	// totalItems is the total number of itesm added, can be used to check completenses after sorting.
	totalItems int64

	// buffers holds the list of buffers that have been created
	buffers []buffers.BufferHandle
}

func NewBigSorter[T BigSortable](factory buffers.BufferFactory, maxBufferSizeBytes int64) *BigSorter[T] {
	return &BigSorter[T]{
		bufferFactory:     factory,
		maxBlockSizeBytes: maxBufferSizeBytes,
	}
}

func (bs *BigSorter[T]) Add(data T) error {
	bs.curBlock = append(bs.curBlock, data)
	bs.curBlockSizeBytes += data.Size()
	if bs.curBlockSizeBytes >= bs.maxBlockSizeBytes {
		if err := bs.storeCurrentBlock(); err != nil {
			return fmt.Errorf("store current block: %w", err)
		}
		bs.curBlock = bs.curBlock[:0]
		bs.curBlockSizeBytes = 0
	}
	bs.totalItems++
	return nil
}

func (bs *BigSorter[T]) Flush() error {
	return bs.storeCurrentBlock()
}

func (bs *BigSorter[T]) storeCurrentBlock() error {
	if len(bs.curBlock) == 0 {
		return nil
	}

	bufHandle, err := bs.bufferFactory.New()
	if err != nil {
		return fmt.Errorf("create buffer for block: %w", err)
	}
	writer, err := bufHandle.GetWriter()
	if err != nil {
		return fmt.Errorf("get writer for buffer: %w", err)
	}

	bufioWriter := bufio.NewWriterSize(writer, 32*1024) // 32 KB buffer
	defer bufioWriter.Flush()
	lenBuf := make([]byte, 4)
	buf := make([]byte, 0, 32*1024) // 32 KB buffer available to serialize items
	for _, item := range bs.curBlock {
		serialized := item.Serialize(buf)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(serialized)))
		_, err := bufioWriter.Write(lenBuf)
		if err != nil {
			return fmt.Errorf("write item length to buffer: %w", err)
		}
		_, err = bufioWriter.Write(serialized)
		if err != nil {
			return fmt.Errorf("write item to buffer: %w", err)
		}
	}
	if err := bufioWriter.Flush(); err != nil {
		return fmt.Errorf("flush buffer writer: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close buffer writer: %w", err)
	}
	bs.buffers = append(bs.buffers, bufHandle)
	return nil
}

func (bs *BigSorter[T]) SortIter() *BigSortIterator[T] {
	if len(bs.curBlock) > 0 {
		panic("cannot create iterator with unflushed data, call Flush() first")
	}
	return &BigSortIterator[T]{
		buffers:     slices.Clone(bs.buffers),
		expectCount: bs.totalItems,
	}
}

// bufferReader is a helper for reading from a buffer and deserializing items of type T.
type bufferReader[T BigSortable] struct {
	reader    io.ReadCloser
	bufReader *bufio.Reader

	buffer []byte
}

func newBufferReader[T BigSortable](reader io.ReadCloser) *bufferReader[T] {
	return &bufferReader[T]{reader: reader, bufReader: bufio.NewReader(reader), buffer: make([]byte, 0, 32*1024)}
}

func (br *bufferReader[T]) Read() (T, error) {
	var zero T

	// Get the length of the next item (4 bytes)
	_, err := io.ReadFull(br.bufReader, br.buffer[:4])
	if err != nil {
		return zero, err
	}
	itemLen := binary.BigEndian.Uint32(br.buffer[:4])

	// Read the item data
	if uint32(cap(br.buffer)) < itemLen {
		newBufSize := itemLen
		if newBufSize < uint32(cap(br.buffer))*2 {
			newBufSize = uint32(cap(br.buffer)) * 2
		}
		br.buffer = make([]byte, newBufSize)
	}

	// Read the item data
	_, err = io.ReadFull(br.bufReader, br.buffer[:itemLen])
	if err != nil {
		return zero, err
	}
	var item T
	if err := item.Deserialize(br.buffer[:itemLen]); err != nil {
		return zero, fmt.Errorf("deserialize item: %w", err)
	}
	return item, nil
}

func (bi *bufferReader[T]) Close() error {
	return bi.reader.Close()
}

type BigSortIterator[T BigSortable] struct {
	buffers     []buffers.BufferHandle
	expectCount int64

	// Error handling
	errOnce  sync.Once
	errReady chan error
	err      error
}

func (bsi *BigSortIterator[T]) Err() error {
	<-bsi.errReady
	return bsi.err
}

func (bsi *BigSortIterator[T]) setError(err error) {
	bsi.errOnce.Do(func() {
		close(bsi.errReady)
		bsi.err = err
	})
}

func (bsi *BigSortIterator[T]) Iter() iter.Seq[T] {
	return func(yield func(T) bool) {
		defer bsi.setError(nil) // signal no error if we exit normally

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const itemChunkSize = 64
		itemChunkReuseChan := make(chan []T, len(bsi.buffers)*32) // buffer for reusable item chunks to reduce allocations

		// Create a goroutine per buffer to read items and send them to their respective channels.
		var wg sync.WaitGroup
		itemChans := make([]chan []T, len(bsi.buffers))
		for i, bufHandle := range bsi.buffers {
			itemChans[i] = make(chan []T, 4) // buffer for item chunks with size 4

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(itemChans[i])

				reader, err := bufHandle.GetReader()
				if err != nil {
					cancel()
					bsi.setError(fmt.Errorf("get reader for buffer %d: %w", i, err))
					return
				}
				defer reader.Close()

				itemReader := newBufferReader[T](reader)
				for {
					// Get a reusable chunk
					var itemChunk []T
					select {
					case itemChunk = <-itemChunkReuseChan:
						itemChunk = itemChunk[:0]
					default:
						itemChunk = make([]T, 0, itemChunkSize)
					}

					// Read items into the chunk
					for len(itemChunk) < cap(itemChunk) {
						item, err := itemReader.Read()
						if err == io.EOF {
							break
						}
						if err != nil {
							cancel()
							bsi.setError(fmt.Errorf("read item from buffer %d: %w", i, err))
							return
						}
						itemChunk = append(itemChunk, item)
					}

					if len(itemChunk) == 0 {
						return // No more items to read
					}

					// Send the chunk to the item channel
					select {
					case itemChans[i] <- itemChunk:
					case <-ctx.Done():
						return // context cancelled
					}
				}
			}()
		}

		var valueHeap valueHeap[T]
		for _, itemChan := range itemChans {
			// Get the first chunk from each channel
			chunk, ok := <-itemChan
			if !ok || len(chunk) == 0 {
				continue // No items in this channel
			}
			valueHeap = append(valueHeap, &valueAndSource[T]{batch: chunk, idx: 0, sourceChan: itemChan, reuseChan: itemChunkReuseChan})
		}
		heap.Init(&valueHeap)

		for valueHeap.Len() > 0 {
			// Pop the smallest item
			minValSrc := heap.Pop(&valueHeap).(*valueAndSource[T])
			currentValue := minValSrc.Current()

			// Yield the current value
			if !yield(currentValue) {
				cancel()
				break
			}

			// Advance the source and push back if it has more items
			if minValSrc.Advance() {
				heap.Push(&valueHeap, minValSrc)
			}
		}

		// Wait for all buffer readers to finish
		wg.Wait()
	}
}
