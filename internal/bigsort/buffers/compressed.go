package buffers

import (
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

func (h *compressedBufferHandle) GetReader() (io.ReadCloser, error) {
	baseReader, err := h.base.GetReader()
	if err != nil {
		return nil, err
	}
	zstdReader, err := zstd.NewReader(baseReader)
	if err != nil {
		return nil, err
	}
	return &closeForwarder{
		base:   baseReader,
		Reader: zstdReader,
	}, nil
}

func (h *compressedBufferHandle) GetWriter() (io.WriteCloser, error) {
	baseWriter, err := h.base.GetWriter()
	if err != nil {
		return nil, err
	}
	zstdWriter, err := zstd.NewWriter(
		baseWriter,
		zstd.WithEncoderConcurrency(2),
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	return zstdWriter, nil
}
