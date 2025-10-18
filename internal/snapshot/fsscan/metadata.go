package fsscan

type FileMetadata struct {
	Path  string
	Size  int64
	Mtime int64
	Flags uint32

	Error error
}
