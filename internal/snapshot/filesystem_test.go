//go:build unix

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryFileSystemImpl_ReadDir(t *testing.T) {
	fs := newMemoryFileSystem()

	fs.AddRegularFile("/a/b/c.txt", 10)
	fs.AddRegularFile("/a/d.txt", 20)
	fs.AddRegularFile("/a/e/f.txt", 30)
	fs.AddRegularFile("/g.txt", 40)
	fs.AddDir("/h")

	testCases := []struct {
		name     string
		path     string
		expected map[string]bool // map of name to isDir
	}{
		{
			name: "root directory",
			path: "/",
			expected: map[string]bool{
				"a":     true,
				"g.txt": false,
				"h":     true,
			},
		},
		{
			name: "subdirectory a",
			path: "/a",
			expected: map[string]bool{
				"b":     true,
				"d.txt": false,
				"e":     true,
			},
		},
		{
			name: "subdirectory a/b",
			path: "/a/b",
			expected: map[string]bool{
				"c.txt": false,
			},
		},
		{
			name: "subdirectory a/e",
			path: "/a/e",
			expected: map[string]bool{
				"f.txt": false,
			},
		},
		{
			name:     "empty directory h",
			path:     "/h",
			expected: map[string]bool{},
		},
		{
			name:     "non-existent directory",
			path:     "/nonexistent",
			expected: map[string]bool{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entries, err := fs.ReadDir(tc.path)
			assert.NoError(t, err)

			results := make(map[string]bool)
			for _, entry := range entries {
				results[entry.Name()] = entry.IsDir()
			}

			assert.Equal(t, tc.expected, results)
		})
	}
}
