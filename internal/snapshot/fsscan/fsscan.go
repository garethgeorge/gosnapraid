package fsscan

import (
	"errors"
	"io/fs"
	"iter"
	"os"
)

var ErrCancelled = errors.New("scan cancelled")

func WalkDirectory(dir string) iter.Seq[FileMetadata] {
	return func(yield func(FileMetadata) bool) {
		if err := fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
			itemPath := dir + string(os.PathSeparator) + path
			if err != nil {
				if !yield(FileMetadata{
					Path:  itemPath,
					Error: err,
				}) {
					return fs.SkipAll
				}
				return nil
			}
			info, err := d.Info()
			if err != nil {
				if !yield(FileMetadata{
					Path:  itemPath,
					Error: err,
				}) {
					return fs.SkipAll
				}
				return nil
			}

			if !yield(FileMetadata{
				Path:  itemPath,
				Mtime: info.ModTime().UnixNano(),
				Size:  info.Size(),
				Mode:  info.Mode(),
				Error: nil,
			}) {
				return fs.SkipAll
			}
			return nil
		}); err != nil && !errors.Is(err, fs.SkipAll) {
			yield(FileMetadata{
				Path:  dir,
				Error: err,
			})
		}
	}
}
