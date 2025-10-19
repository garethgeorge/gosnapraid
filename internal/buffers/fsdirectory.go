package buffers

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

// global atomic counter for buffer IDs, means multiple dirBufferFactory instances
// can safely create buffers in the same directory without colliding.
var (
	nextFsBufID atomic.Int64
)

// dirBufferFactory
type dirBufferFactory struct {
	dir string
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
	id := nextFsBufID.Add(1)
	filePath := fmt.Sprintf("%s/%04d.buf", d.dir, id)
	return &fileBuffer{fpath: filePath}, nil
}

func (d *dirBufferFactory) Release() error {
	return os.RemoveAll(d.dir)
}

type fileBuffer struct {
	fpath string
}

func (f *fileBuffer) Name() string {
	return f.fpath
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
