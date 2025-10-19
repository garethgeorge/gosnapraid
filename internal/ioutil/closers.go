package ioutil

import "io"

func WithWriterCloser(w io.Writer, closer func() error) io.WriteCloser {
	return &writeCloser{
		Writer: w,
		closer: closer,
	}
}

type writeCloser struct {
	io.Writer
	closer func() error
}

var _ io.WriteCloser = (*writeCloser)(nil)

func (wc *writeCloser) Close() error {
	return wc.closer()
}

func WithReaderCloser(r io.Reader, closer func() error) io.ReadCloser {
	return &readCloser{
		Reader: r,
		closer: closer,
	}
}

type readCloser struct {
	io.Reader
	closer func() error
}

var _ io.ReadCloser = (*readCloser)(nil)

func (rc *readCloser) Close() error {
	return rc.closer()
}
