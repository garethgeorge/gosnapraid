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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/garethgeorge/gosnapraid/internal/bigsort/buffers"
	"github.com/garethgeorge/gosnapraid/internal/poolutil"
)

type options struct {
	maxBufferSizeBytes int64
	addParallelism     int
	iterChunkSize      int
}

type Option = func(*options)

// WithMaxBufferSizeBytes sets the maximum size of each buffer block before flushing to storage.
func WithMaxBufferSizeBytes(size int64) func(*options) {
	return func(o *options) {
		o.maxBufferSizeBytes = size
	}
}

// WithAddParallelism sets the number of parallel goroutines used to add blocks.
func WithAddParallelism(parallelism int) func(*options) {
	return func(o *options) {
		o.addParallelism = parallelism
	}
}

// WithIterChunkSize sets the number of items to read ahead from each buffer during iteration.
// Larger chunk sizes can improve performance at the cost of higher memory usage.
// At most 2 blocks of 'chunkSize' will be held in memory per buffer during iteration. 1 in use and one buffered.
func WithIterChunkSize(chunkSize int) func(*options) {
	return func(o *options) {
		o.iterChunkSize = chunkSize
	}
}

// BigSorter is a generic sorter that can handle large amounts of data by breaking it into sorted
// blocks and then performing a k-way merge during iteration.
//
// T is the type of the items to be sorted. PT is a pointer to T that implements the BigSortable interface.
//
// BigSorter is thread safe for adding items, flushing, and iteration.
//
// Add items using Add(), then call Flush() to ensure all data is written. Errors during Add() and Flush() must be checked.
// After flushing, create an iterator using SortIter() to read sorted items, always check for errors from the iterator using Err() after iteration.
type BigSorter[T any, PT interface {
	*T
	BigSortable
}] struct {
	// bufferFactory is used to create new buffers for storing sorted blocks
	bufferFactory buffers.BufferFactory

	// maxBlockSizeBytes is the maximum size of each block buffer before flushing to storage
	maxBlockSizeBytes int64

	// iterChunkSize is the chunk size used during iteration to read items from buffers
	iterChunkSize int

	// curBlockSizeBytes is the current size of the block being built
	curBlockMu        sync.Mutex
	curBlockSizeBytes int64
	curBlock          []T // Stores the actual values, not pointers

	// totalItems is the total number of items added, can be used to check completeness after sorting.
	totalItems int

	// buffers holds the list of buffers that have been created
	buffersMu sync.Mutex
	buffers   []buffers.BufferHandle

	addBlockWg   sync.WaitGroup
	addBlockChan chan []T

	err atomic.Pointer[error]
}

func NewBigSorter[T any, PT interface {
	*T
	BigSortable
}](factory buffers.BufferFactory, opts ...func(*options)) *BigSorter[T, PT] {
	options := options{
		maxBufferSizeBytes: 32 * 1024 * 1024, // 32 MB
		addParallelism:     2,
		iterChunkSize:      64,
	}

	for _, opt := range opts {
		opt(&options)
	}

	sorter := &BigSorter[T, PT]{
		bufferFactory:     factory,
		maxBlockSizeBytes: options.maxBufferSizeBytes,
		iterChunkSize:     options.iterChunkSize,
		addBlockWg:        sync.WaitGroup{},
		addBlockChan:      make(chan []T),
	}

	go func() {
		for block := range sorter.addBlockChan {
			if err := sorter.storeBlock(block); err != nil {
				sorter.setError(fmt.Errorf("store block: %w", err))
			}
			sorter.addBlockWg.Done()
		}
	}()

	return sorter
}

func (bs *BigSorter[T, PT]) setError(err error) {
	bs.err.CompareAndSwap(nil, &err)
}

func (bs *BigSorter[T, PT]) haveError() error {
	if errPtr := bs.err.Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

func (bs *BigSorter[T, PT]) Close() error {
	// Wait for inflight blocks then close the add block channel, will panic if called twice
	bs.addBlockWg.Wait()
	close(bs.addBlockChan)

	// Release buffer storage
	bs.buffersMu.Lock()
	defer bs.buffersMu.Unlock()
	bs.buffers = nil
	if err := bs.bufferFactory.Release(); err != nil {
		return fmt.Errorf("release buffer factory: %w", err)
	}

	// Note that we don't check the error state, caller should get the error from Add() and Flush() call sequence
	return nil
}

func (bs *BigSorter[T, PT]) Add(data T) error {
	bs.curBlockMu.Lock()
	defer bs.curBlockMu.Unlock()

	// Check for errors before proceeding
	if err := bs.haveError(); err != nil {
		return err
	}

	// Append data to current block
	bs.curBlock = append(bs.curBlock, data)
	var dataPtr PT = &data
	bs.curBlockSizeBytes += dataPtr.Size()

	// Check if we need to flush the block, if we do so, swap to a new block
	// and send the old one to the work queue for storage.
	if bs.curBlockSizeBytes >= bs.maxBlockSizeBytes {
		bs.swapToNewBlock()
	}
	bs.totalItems++
	return nil
}

func (bs *BigSorter[T, PT]) TotalItems() int {
	return bs.totalItems
}

func (bs *BigSorter[T, PT]) Flush() error {
	bs.swapToNewBlock()
	bs.addBlockWg.Wait()
	if err := bs.haveError(); err != nil {
		return fmt.Errorf("block failed: %w", err)
	}
	return nil
}

func (bs *BigSorter[T, PT]) swapToNewBlock() {
	bs.addBlockWg.Add(1)
	bs.addBlockChan <- bs.curBlock

	bs.curBlock = make([]T, 0, len(bs.curBlock))
	bs.curBlockSizeBytes = 0
}

func (bs *BigSorter[T, PT]) storeBlock(block []T) error {
	if len(block) == 0 {
		return nil
	}

	sort.Slice(block, func(i, j int) bool {
		ptrI := PT(&block[i])
		ptrJ := PT(&block[j])
		return ptrI.Less(ptrJ)
	})

	bufHandle, err := bs.bufferFactory.New()
	if err != nil {
		return fmt.Errorf("create buffer for block: %w", err)
	}
	writer, err := bufHandle.GetWriter()
	if err != nil {
		return fmt.Errorf("get writer for buffer: %w", err)
	}
	defer writer.Close()

	bufioWriter := bufio.NewWriterSize(writer, 32*1024) // 32 KB buffer
	lenBuf := make([]byte, 4)
	buf := make([]byte, 0, 32*1024) // 32 KB buffer available to serialize items
	for i := range block {
		// Get a pointer to the item in the slice to call Serialize
		itemPtr := PT(&block[i])
		serialized := itemPtr.Serialize(buf)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(serialized)))
		if _, err := bufioWriter.Write(lenBuf); err != nil {
			return fmt.Errorf("write item length to buffer: %w", err)
		}
		if _, err = bufioWriter.Write(serialized); err != nil {
			return fmt.Errorf("write item to buffer: %w", err)
		}
	}

	if err := bufioWriter.Flush(); err != nil {
		return fmt.Errorf("flush buffer writer: %w", err)
	}

	bs.buffersMu.Lock()
	defer bs.buffersMu.Unlock()
	bs.buffers = append(bs.buffers, bufHandle)
	return nil
}

func (bs *BigSorter[T, PT]) SortIter() *BigSortIterator[T, PT] {
	if len(bs.curBlock) > 0 {
		panic("cannot create iterator with unflushed data, call Flush() first")
	}
	bs.buffersMu.Lock()
	defer bs.buffersMu.Unlock()
	return &BigSortIterator[T, PT]{
		buffers:       slices.Clone(bs.buffers),
		iterChunkSize: bs.iterChunkSize,
		expectCount:   bs.totalItems,
		errReady:      make(chan error, 1), // Buffered to prevent blocking
	}
}

// bufferReader is a helper for reading from a buffer and deserializing items of type T.
type bufferReader[T any, PT interface {
	*T
	BigSortable
}] struct {
	reader    io.ReadCloser
	bufReader *bufio.Reader
	buffer    []byte
}

func newBufferReader[T any, PT interface {
	*T
	BigSortable
}](reader io.ReadCloser) *bufferReader[T, PT] {
	return &bufferReader[T, PT]{reader: reader, bufReader: bufio.NewReader(reader), buffer: make([]byte, 4, 32*1024)}
}

func (br *bufferReader[T, PT]) Read() (T, error) {
	var zero T

	// Read the length of the next item
	_, err := io.ReadFull(br.bufReader, br.buffer[:4])
	if err != nil {
		return zero, err
	}
	itemLen := binary.BigEndian.Uint32(br.buffer[:4])

	// Ensure buffer is large enough
	if uint32(cap(br.buffer)) < itemLen {
		br.buffer = make([]byte, itemLen)
	} else {
		br.buffer = br.buffer[:itemLen]
	}

	// Read the item data
	if _, err = io.ReadFull(br.bufReader, br.buffer); err != nil {
		return zero, err
	}

	// Create a new value and get its pointer to deserialize into.
	var item T
	var itemPtr PT = &item
	if err := itemPtr.Deserialize(br.buffer); err != nil {
		return zero, fmt.Errorf("deserialize item: %w", err)
	}
	return item, nil
}

func (br *bufferReader[T, PT]) Close() error {
	return br.reader.Close()
}

type BigSortIterator[T any, PT interface {
	*T
	BigSortable
}] struct {
	buffers       []buffers.BufferHandle
	expectCount   int
	iterChunkSize int

	// Error handling
	errReady chan error
	err      atomic.Pointer[error]
}

func (bsi *BigSortIterator[T, PT]) Err() error {
	<-bsi.errReady // Wait for iterator to finish
	if errPtr := bsi.err.Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

func (bsi *BigSortIterator[T, PT]) setError(err error) {
	if bsi.err.CompareAndSwap(nil, &err) {
		close(bsi.errReady)
	}
}

func (bsi *BigSortIterator[T, PT]) Iter() iter.Seq[T] {
	return func(yield func(T) bool) {
		defer bsi.setError(nil) // signal no error if we exit normally

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Buffer for reusable item chunks to reduce allocations
		itemChunkPool := poolutil.NewPool[[]T](func() []T {
			return make([]T, 0, bsi.iterChunkSize)
		}, func(val []T) []T {
			return val[:0]
		}, bsi.iterChunkSize*len(bsi.buffers)) // Enough for one chunk per buffer

		var wg sync.WaitGroup
		itemChans := make([]chan []T, len(bsi.buffers))
		for i := range bsi.buffers {
			itemChans[i] = make(chan []T, 1) // Buffered to allow one chunk ahead
			wg.Add(1)
			go func(i int, bufHandle buffers.BufferHandle) {
				defer wg.Done()
				defer close(itemChans[i])

				reader, err := bufHandle.GetReader()
				if err != nil {
					cancel()
					bsi.setError(fmt.Errorf("get reader for buffer %d: %w", i, err))
					return
				}
				defer reader.Close()

				itemReader := newBufferReader[T, PT](reader)
				for {
					itemChunk := itemChunkPool.Get()

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
						return
					}

					select {
					case itemChans[i] <- itemChunk:
					case <-ctx.Done():
						return
					}
				}
			}(i, bsi.buffers[i])
		}

		var valueHeap valueHeap[T, PT]
		for _, itemChan := range itemChans {
			chunk, ok := <-itemChan
			if !ok || len(chunk) == 0 {
				continue
			}
			valueHeap = append(valueHeap, &valueAndSource[T, PT]{
				batch:      chunk,
				idx:        0,
				sourceChan: itemChan,
				pool:       itemChunkPool,
			})
		}
		heap.Init(&valueHeap)

		read := 0
		for valueHeap.Len() > 0 {
			minValSrc := heap.Pop(&valueHeap).(*valueAndSource[T, PT])
			read++
			if !yield(minValSrc.Current()) {
				read = bsi.expectCount // to avoid error on early stop
				break
			}
			if minValSrc.Advance() {
				heap.Push(&valueHeap, minValSrc)
			}
		}
		cancel()
		wg.Wait()
		if read < bsi.expectCount {
			bsi.setError(fmt.Errorf("iteration stopped early after %d of %d items", read, bsi.expectCount))
		}
	}
}

type valueAndSource[T any, PT interface {
	*T
	BigSortable
}] struct {
	batch      []T
	idx        int
	sourceChan chan []T
	pool       *poolutil.Pool[[]T]
}

func (v *valueAndSource[T, PT]) Current() T {
	return v.batch[v.idx]
}

func (v *valueAndSource[T, PT]) Advance() bool {
	v.idx++
	if v.idx < len(v.batch) {
		return true
	}
	v.pool.Put(v.batch)
	newBatch, ok := <-v.sourceChan
	if !ok || len(newBatch) == 0 {
		return false
	}
	v.batch = newBatch
	v.idx = 0
	return true
}

type valueHeap[T any, PT interface {
	*T
	BigSortable
}] []*valueAndSource[T, PT]

func (h valueHeap[T, PT]) Len() int { return len(h) }

func (h valueHeap[T, PT]) Less(i, j int) bool {
	itemI := PT(&h[i].batch[h[i].idx])
	itemJ := PT(&h[j].batch[h[j].idx])
	return itemI.Less(itemJ)
}

func (h valueHeap[T, PT]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *valueHeap[T, PT]) Push(x any) {
	*h = append(*h, x.(*valueAndSource[T, PT]))
}

func (h *valueHeap[T, PT]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}
