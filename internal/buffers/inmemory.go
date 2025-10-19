package buffers

import (
	"bytes"
	"io"
)

// inMemoryBufferFactory is a factory for creating in-memory buffers.
type inMemoryBufferFactory struct {
}

func NewInMemoryBufferFactory() BufferFactory {
	return &inMemoryBufferFactory{}
}

func (p *inMemoryBufferFactory) New() (BufferHandle, error) {
	return &inMemoryBuffer{}, nil
}

func (p *inMemoryBufferFactory) Release() error {
	return nil
}

// inMemoryBuffer is an in-memory implementation of bufferHandle.
type inMemoryBuffer struct {
	data []byte
}

var _ io.WriteCloser = (*inMemoryBuffer)(nil)
var _ RawBufferHandle = (*inMemoryBuffer)(nil)

func (b *inMemoryBuffer) Name() string {
	return "inmemory"
}

func (b *inMemoryBuffer) GetReader() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

func (b *inMemoryBuffer) GetWriter() (io.WriteCloser, error) {
	b.data = b.data[:0]
	return b, nil
}

func (b *inMemoryBuffer) Write(p []byte) (n int, err error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *inMemoryBuffer) Close() error {
	return nil
}

func (b *inMemoryBuffer) isRaw() bool {
	return true
}
