package buffers

import (
	"io"
	"os"
	"path/filepath"
)

// bufferHandle is a handle to a store of data that can be read from and written to.
type BufferHandle interface {
	GetReader() (io.ReadCloser, error)
	GetWriter() (io.WriteCloser, error)
	Name() string
}

// bufferFactory is a factory for creating bufferHandles and can release any resources when done.
type BufferFactory interface {
	New() (BufferHandle, error)
	Release() error
}

type BufferFactoryFactory func() (BufferFactory, error)

func NewCompressedInMemoryBufferFactoryFactory() BufferFactoryFactory {
	return func() (BufferFactory, error) {
		return NewCompressedBufferFactory(NewInMemoryBufferFactory()), nil
	}
}

func NewCompressedDirBufferFactoryFactory(dir string) BufferFactoryFactory {
	return func() (BufferFactory, error) {
		if dir == "" {
			dir = filepath.Join(os.TempDir(), "bigsortdiskbuffers")
		}
		err := os.MkdirAll(dir, 0o755)
		if err != nil {
			return nil, err
		}
		dirBufFactory, err := NewDirBufferFactory(dir)
		if err != nil {
			return nil, err
		}
		return NewCompressedBufferFactory(dirBufFactory), nil
	}
}
