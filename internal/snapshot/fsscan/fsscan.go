package fsscan

type FileSystemScanner struct {
	rootDirs []string

	dirQueue []string
}

func NewFileSystemScanner(rootDirs []string) *FileSystemScanner {
	return &FileSystemScanner{
		rootDirs: rootDirs,
	}
}

func (fss *FileSystemScanner) Close() error {
	// No resources to clean up in this implementation.
	return nil
}
