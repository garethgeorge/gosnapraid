package buffers

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferFactories(t *testing.T) {
	t.Parallel()
	// factories is a map of functions that create buffer factories.
	// This allows us to test different factory implementations with the same test suite.
	factories := map[string]func(t *testing.T) BufferFactory{
		"inMemory": func(t *testing.T) BufferFactory {
			return NewInMemoryBufferFactory()
		},
		"inMemoryCompressed": func(t *testing.T) BufferFactory {
			return NewCompressedBufferFactory(NewInMemoryBufferFactory())
		},
		"dir": func(t *testing.T) BufferFactory {
			dir := t.TempDir()
			factory, err := NewDirBufferFactory(dir)
			require.NoError(t, err)
			return factory
		},
		"dirCompressed": func(t *testing.T) BufferFactory {
			dir := t.TempDir()
			baseFactory, err := NewDirBufferFactory(dir)
			require.NoError(t, err)
			return NewCompressedBufferFactory(baseFactory)
		},
	}

	for name, factoryFn := range factories {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			factory := factoryFn(t)

			// 1. Assert that at least 3 buffers can be made for each.
			var handles []BufferHandle
			for i := 0; i < 3; i++ {
				handle, err := factory.New()
				require.NoError(t, err, "failed to create buffer %d", i)
				handles = append(handles, handle)
			}
			require.Len(t, handles, 3, "expected 3 handles to be created")

			// 2. Assert that values can be written to and read back identically.
			for i, handle := range handles {
				t.Run(fmt.Sprintf("buffer-%d", i), func(t *testing.T) {
					testData := []byte(fmt.Sprintf("some test data for buffer %d", i))

					// Write data to the buffer
					writer, err := handle.GetWriter()
					require.NoError(t, err)
					n, err := writer.Write(testData)
					require.NoError(t, err)
					assert.Equal(t, len(testData), n)
					require.NoError(t, writer.Close())

					// Read data back from the buffer
					reader, err := handle.GetReader()
					require.NoError(t, err)
					readData, err := io.ReadAll(reader)
					require.NoError(t, err)
					require.NoError(t, reader.Close())

					// Assert the data is the same
					assert.Equal(t, testData, readData)
				})
			}

			t.Run("release after use", func(t *testing.T) {
				assert.NoError(t, factory.Release())
			})
		})
	}
}
