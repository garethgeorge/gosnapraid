package ioutil

import (
	"bufio"
	"io"
)

const DefaultBufioSize = 64 * 1024 // 64KB

func WithBufferedWrites(w io.Writer) io.WriteCloser {
	bufw := bufio.NewWriterSize(w, DefaultBufioSize)
	return WithWriterCloser(bufw, func() error {
		if err := bufw.Flush(); err != nil {
			return err
		}
		if c, ok := w.(io.Closer); ok {
			return c.Close()
		}
		return nil
	})
}

func WithBufferedReads(r io.Reader) io.Reader {
	return bufio.NewReaderSize(r, DefaultBufioSize)
}
