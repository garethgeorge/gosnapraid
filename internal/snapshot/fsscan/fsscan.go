package fsscan

import (
	"errors"
	"io/fs"
	"iter"
	"os"
)

var ErrCancelled = errors.New("scan cancelled")

// WalkFS walks the filesystem starting from the root directory.
// This is useful for testing with custom filesystems or for fuzzing.
func WalkFS(fsys fs.FS) iter.Seq[FileMetadata] {
	return func(yield func(FileMetadata) bool) {
		if err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if !yield(FileMetadata{
					Path:  path,
					Error: err,
				}) {
					return fs.SkipAll
				}
				return nil
			}
			info, err := d.Info()
			if err != nil {
				if !yield(FileMetadata{
					Path:  path,
					Error: err,
				}) {
					return fs.SkipAll
				}
				return nil
			}

			if !yield(FileMetadata{
				Path:  path,
				Mtime: info.ModTime().UnixNano(),
				Size:  info.Size(),
				Mode:  info.Mode(),
				Error: nil,
			}) {
				return fs.SkipAll
			}
			return nil
		}); err != nil {
			yield(FileMetadata{
				Path:  "",
				Error: err,
			})
		}
	}
}

// WalkDirectory walks a directory on the OS filesystem.
// For testing with custom filesystems, use WalkFS instead.
func WalkDirectory(dir string) iter.Seq[FileMetadata] {
	return WalkFS(os.DirFS(dir))
}
