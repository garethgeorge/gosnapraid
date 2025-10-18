package fsscan

import (
	"context"
)

type FileMetadata struct {
	Path  string
	Size  int64
	Mtime int64
	Flags uint32

	Error error
}

type workItem struct {
	path   string
	err chan error
	result chan []FileMetadata
}

func newWorkItem(path string) workItem {
	return workItem{
		path:   path,
		err:    make(chan error),
		result: make(chan []FileMetadata),
	}
}

func processWorkItems(ctx context.Context, work chan workItem) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-work:
			if !ok {
				return
			}
			// Process the work item
			dirEnts, err := os.ReadDir(item.path)
			if err != nil {
				item.result <- []FileMetadata{{Path: item.path, Error: err}}
				continue
			}

			var results []FileMetadata

func ScanDirectory(ctx context.Context, rootPath string) (chan FileMetadata, chan error) {
	files := make(chan FileMetadata)
	errors := make(chan error)

}
