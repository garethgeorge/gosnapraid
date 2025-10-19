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
		return WithWriterCloser(io.Discard, func() error { return nil })
	}
	if len(writers) == 1 {
		return WithBufferedWrites(writers[0])
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

	multiwriter := io.MultiWriter(pipeWriters...)
	return WithBufferedWrites(WithWriterCloser(multiwriter, func() error {
		var err error
		for _, w := range pipeClosers {
			if e := w.Close(); e != nil {
				err = e
			}
		}
		if e := eg.Wait(); e != nil {
			err = e
		}
		return err
	}))
}
