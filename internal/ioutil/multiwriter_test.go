package ioutil

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelMultiWriter(t *testing.T) {
	t.Run("no writers", func(t *testing.T) {
		wc := ParallelMultiWriter()
		n, err := wc.Write([]byte("hello"))
		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.NoError(t, wc.Close())
	})

	t.Run("one writer", func(t *testing.T) {
		var buf bytes.Buffer
		wc := ParallelMultiWriter(&buf)
		n, err := wc.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.NoError(t, wc.Close())
		assert.Equal(t, "hello", buf.String())
	})

	t.Run("multiple writers", func(t *testing.T) {
		var buf1 bytes.Buffer
		var buf2 bytes.Buffer
		var buf3 bytes.Buffer
		wc := ParallelMultiWriter(&buf1, &buf2, &buf3)
		n, err := wc.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.NoError(t, wc.Close())
		assert.Equal(t, "hello", buf1.String())
		assert.Equal(t, "hello", buf2.String())
		assert.Equal(t, "hello", buf3.String())
	})

	t.Run("multiple writers with a failing writer", func(t *testing.T) {
		var buf1 bytes.Buffer
		failingWriter := &failingWriter{}
		var buf3 bytes.Buffer
		wc := ParallelMultiWriter(&buf1, failingWriter, &buf3)
		n, err := wc.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, 5, n)

		// Close will return an error because one of the writers failed.
		err = wc.Close()
		assert.Error(t, err)

		// The other writers should have received the data.
		assert.Equal(t, "hello", buf1.String())
		assert.Equal(t, "hello", buf3.String())
	})
}

// failingWriter is a writer that always fails on write.
type failingWriter struct{}

func (fw *failingWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrShortWrite
}
