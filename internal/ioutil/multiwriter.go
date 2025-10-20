package ioutil

import (
	"io"

	"golang.org/x/sync/errgroup"
)

// ParallelMultiWriter creates a writer that writes to multiple writes in parallel.
// Uses an internal pipe to each writer to allow parallel writes.
// Uses a 64KB internal buffer to reduce the number of writes to each writer.
//
// Ensure .Close is called on the returned writer to flush and close all writers
// and release resources.
func ParallelMultiWriter(writers ...io.Writer) io.WriteCloser {
	if len(writers) == 0 {
		return WriterWithCloser(io.Discard, NewMultiCloser())
	}
	if len(writers) == 1 {
		return WriterWithCloser(writers[0], NewMultiCloser())
	}

	var eg errgroup.Group
	var pipeWriters []io.Writer
	var pipeClosers []io.Closer

	for _, w := range writers {
		pr, pw := io.Pipe()
		pipeWriters = append(pipeWriters, pw)
		pipeClosers = append(pipeClosers, pw)
		eg.Go(func(w io.Writer, r io.Reader) func() error {
			return func() error {
				buffer := make([]byte, DefaultBufioSize) // will exactly match WithBufferedWrites buffer size
				_, err := io.CopyBuffer(w, r, buffer)
				return err
			}
		}(w, pr))
	}

	multiwriter := WithBufferedWrites(io.MultiWriter(pipeWriters...))
	pipeClosers = append(pipeClosers, multiwriter)
	return WriterWithCloser(multiwriter, NewMultiCloser(pipeClosers...))
}
