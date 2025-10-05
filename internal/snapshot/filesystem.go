//go:build unix

package snapshot

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/garethgeorge/gosnapraid/internal/errors"
)

type fileSystemDirIter struct {
	entries []os.DirEntry
	idx     int
}

// FileSystemIterator is very deliberately written to closely resemble the interface exposed by the
// snapshot reader / writer.
//
// A key requirement is that both iterators provide the same ordering of nodes.
// The ordering is depth-first and the entries on each level are returned in lexographical order by name.
type FileSystemIterator struct {
	rootDirectory string
	iters         []*fileSystemDirIter

	curDir []os.DirEntry

	errorAgg errors.ErrorAggregation
}

func NewFileSystemIterator(rootDirectory string) (*FileSystemIterator, error) {
	// Read the root directory and create the initial directory iterator
	entries, err := os.ReadDir(rootDirectory)
	if err != nil {
		return nil, err
	}

	// Create the initial directory iterator
	iter := fileSystemDirIter{
		entries: entries,
		idx:     0,
	}

	return &FileSystemIterator{
		rootDirectory: rootDirectory,
		iters:         []*fileSystemDirIter{&iter},
	}, nil
}

func (f *FileSystemIterator) Errors() errors.ErrorAggregation {
	return f.errorAgg
}

func (f *FileSystemIterator) CurrentDirPath() string {
	return f.rootDirectory + string(os.PathSeparator) + f.CurrentDirRelativePath()
}

func (f *FileSystemIterator) CurrentDirRelativePath() string {
	var segments []string
	for _, parent := range f.iters {
		segments = append(segments, parent.entries[parent.idx].Name())
	}
	return strings.Join(segments, string(os.PathSeparator))
}

func (f *FileSystemIterator) Siblings() []os.DirEntry {
	return f.curDir
}

func (f *FileSystemIterator) Parents() []os.DirEntry {
	parents := make([]os.DirEntry, len(f.iters))
	for i, iter := range f.iters {
		parents[i] = iter.entries[iter.idx]
	}
	return parents
}

func (f *FileSystemIterator) NextDirectory() error {
	for {
		if len(f.iters) == 0 {
			return ErrNoMoreNodes
		}
		citr := f.iters[len(f.iters)-1]

		// If the iterator is exhausted, we've managed to reach the end of a directory. Yield to allow the caller to process the directory.
		if citr.idx >= len(citr.entries) {
			f.curDir = citr.entries
			f.iters = f.iters[:len(f.iters)-1]
			return nil
		}

		// Otherwise, let's look at the entry. If it's a file, do nothing. If it's a directory, read children and push the iterator onto the stack.
		// This is how we implement a depth-first traversal.
		ent := citr.entries[citr.idx]
		citr.idx++
		if ent.IsDir() {
			if ent.Name() == "." || ent.Name() == ".." {
				continue // Skip . and ..
			}

			dirPath := f.CurrentDirPath() + string(os.PathSeparator) + ent.Name()
			entries, err := os.ReadDir(dirPath)
			if err != nil {
				f.errorAgg.Add(errors.NewFixedCause("read dir", fmt.Errorf("for dir %s: %w", dirPath, err)))
				continue
			}
			// Sort the entries by name.
			slices.SortFunc(entries, func(a, b os.DirEntry) int {
				return strings.Compare(a.Name(), b.Name())
			})
			f.iters = append(f.iters, &fileSystemDirIter{
				entries: entries,
				idx:     0,
			})
			continue
		}
	}
}
