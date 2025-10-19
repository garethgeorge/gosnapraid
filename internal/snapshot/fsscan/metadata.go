package fsscan

import "os"

type FileMetadata struct {
	Path  string
	Size  int64
	Mtime int64
	Mode  os.FileMode
	Error error
}
