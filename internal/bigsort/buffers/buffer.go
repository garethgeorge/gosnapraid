package buffers

import (
	"io"
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
