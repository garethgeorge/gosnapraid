package buffers

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// bufferHandle is a handle to a store of data that can be read from and written to.
type BufferHandle interface {
	GetReader() (io.ReadCloser, error)
	GetWriter() (io.WriteCloser, error)
}

// bufferFactory is a factory for creating bufferHandles and can release any resources when done.
type BufferFactory interface {
	New() (BufferHandle, error)
	Release() error
}

// inMemoryBufferFactory is a factory for creating in-memory buffers.
type inMemoryBufferFactory struct {
	mu      sync.Mutex
	buffers []*inMemoryBuffer
}

func NewInMemoryBufferFactory() BufferFactory {
	return &inMemoryBufferFactory{}
}

func (p *inMemoryBufferFactory) New() (BufferHandle, error) {
	buf := &inMemoryBuffer{}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buffers = append(p.buffers, buf)
	return buf, nil
}

func (p *inMemoryBufferFactory) Release() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buffers = nil
	return nil
}

// inMemoryBuffer is an in-memory implementation of bufferHandle.
type inMemoryBuffer struct {
	data []byte
}

var _ io.WriteCloser = (*inMemoryBuffer)(nil)

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

// dirBufferFactory
type dirBufferFactory struct {
	dir    string
	nextID atomic.Int64
}

func newDirBufferFactory(dir string) (*dirBufferFactory, error) {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	return &dirBufferFactory{dir: dir}, nil
}

func NewDirBufferFactory(dir string) (BufferFactory, error) {
	return newDirBufferFactory(dir)
}

func (d *dirBufferFactory) New() (BufferHandle, error) {
	// create a file in the directory
	id := d.nextID.Add(1)
	filePath := fmt.Sprintf("%s/%04d.buf", d.dir, id)
	return &fileBuffer{fpath: filePath}, nil
}

func (d *dirBufferFactory) Release() error {
	return os.RemoveAll(d.dir)
}

type fileBuffer struct {
	fpath string
}

func (f *fileBuffer) GetWriter() (io.WriteCloser, error) {
	fh, err := os.OpenFile(f.fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return fh, nil
}

func (f *fileBuffer) GetReader() (io.ReadCloser, error) {
	fh, err := os.OpenFile(f.fpath, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return fh, nil
}

// closeForwarder allows forwarding close calls to a base closer
type closeForwarder struct {
	base io.Closer
	io.Reader
}

func (c *closeForwarder) Close() error {
	return c.base.Close()
}
