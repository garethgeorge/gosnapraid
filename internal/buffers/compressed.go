package buffers

import (
	"bufio"
	"io"

	"github.com/klauspost/compress/zstd"
)

// compressedBufferFactory is a bufferFactory that compresses data.
type compressedBufferFactory struct {
	baseFactory BufferFactory
}

func NewCompressedBufferFactory(baseFactory BufferFactory) BufferFactory {
	return &compressedBufferFactory{baseFactory: baseFactory}
}

var _ BufferFactory = (*compressedBufferFactory)(nil)

func (f *compressedBufferFactory) New() (BufferHandle, error) {
	baseHandle, err := f.baseFactory.New()
	if err != nil {
		return nil, err
	}
	return &compressedBufferHandle{base: baseHandle}, nil
}

func (f *compressedBufferFactory) Release() error {
	return f.baseFactory.Release()
}

// compressedBufferHandle is a bufferHandle that compresses data.
type compressedBufferHandle struct {
	base BufferHandle
}

func (h *compressedBufferHandle) Name() string {
	return "zstd+" + h.base.Name()
}

func (h *compressedBufferHandle) GetReader() (io.ReadCloser, error) {
	baseReader, err := h.base.GetReader()
	if err != nil {
		return nil, err
	}
	zstdReader, err := zstd.NewReader(baseReader)
	if err != nil {
		return nil, err
	}
	bufioReader := bufio.NewReaderSize(zstdReader, 64*1024)
	return &readerCloseForwarder{
		closers: []func() error{func() error {
			zstdReader.Close()
			return nil
		}, baseReader.Close},
		Reader: bufioReader,
	}, nil
}

func (h *compressedBufferHandle) GetWriter() (io.WriteCloser, error) {
	baseWriter, err := h.base.GetWriter()
	if err != nil {
		return nil, err
	}
	bufioWriter := bufio.NewWriterSize(baseWriter, 64*1024)

	zstdWriter, err := zstd.NewWriter(
		bufioWriter,
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(2),
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	return &writerCloseForwarder{
		closers:     []func() error{zstdWriter.Close, bufioWriter.Flush, baseWriter.Close},
		WriteCloser: zstdWriter,
	}, nil
}
