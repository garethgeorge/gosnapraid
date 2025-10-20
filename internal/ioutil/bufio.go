package ioutil

import (
	"bufio"
	"io"
)

const DefaultBufioSize = 64 * 1024 // 64KB

func WithBufferedWrites(w io.Writer) io.WriteCloser {
	bufw := bufio.NewWriterSize(w, DefaultBufioSize)
	return WriterWithCloser(bufw, CloserFunc(bufw.Flush))
}

func WithBufferedReads(r io.Reader) io.Reader {
	return bufio.NewReaderSize(r, DefaultBufioSize)
}
