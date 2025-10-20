package ioutil

import "io"

type multiCloser struct {
	closers []io.Closer
}

var _ io.Closer = (*multiCloser)(nil)

func (mc *multiCloser) Close() error {
	var err error
	for _, c := range mc.closers {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return err
}

func NewMultiCloser(closers ...io.Closer) io.Closer {
	return &multiCloser{
		closers: closers,
	}
}

type writeCloser struct {
	io.Writer
	io.Closer
}

var _ io.WriteCloser = (*writeCloser)(nil)

func WriterWithCloser(w io.Writer, c io.Closer) io.WriteCloser {
	return &writeCloser{
		Writer: w,
		Closer: c,
	}
}

type readCloser struct {
	io.Reader
	io.Closer
}

var _ io.ReadCloser = (*readCloser)(nil)

func ReaderWithCloser(r io.Reader, c io.Closer) io.ReadCloser {
	return &readCloser{
		Reader: r,
		Closer: c,
	}
}

// CloserFunc creates an io.Closer from a function
func CloserFunc(closeFunc func() error) io.Closer {
	return &funcCloser{
		closeFunc: closeFunc,
	}
}

type funcCloser struct {
	closeFunc func() error
}

var _ io.Closer = (*funcCloser)(nil)

func (fc *funcCloser) Close() error {
	return fc.closeFunc()
}
