package buffers

import (
	"io"
)

// closeForwarder allows forwarding close calls to a base closer
type readerCloseForwarder struct {
	closers []func() error
	io.Reader
}

func (c *readerCloseForwarder) Close() error {
	var err error
	for _, closer := range c.closers {
		if e := closer(); e != nil {
			err = e
		}
	}
	return err
}

type writerCloseForwarder struct {
	closers []func() error
	io.WriteCloser
}

func (c *writerCloseForwarder) Close() error {
	var err error
	for _, closer := range c.closers {
		if e := closer(); e != nil {
			err = e
		}
	}
	return err
}
