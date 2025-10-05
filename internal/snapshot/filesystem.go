//go:build unix

package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	ErrMetadataNotSupported = errors.New("metadata not supported")
)

type unixMetadata struct {
	Ino        uint64
	Gen        uint64
	AccessTime int64 // microseconds since epoch
	ChangeTime int64 // microseconds since epoch
	BirthTime  int64 // microseconds since epoch
	UID        uint32
	GID        uint32
	DeviceID   uint64
}

type FileSystem interface {
	ReadDir(path string) ([]os.DirEntry, error)
	GetUnixMetadata(info os.FileInfo) (metadata unixMetadata, err error)
}

// memoryFileInfo implements os.FileInfo for the in-memory file system.
type memoryFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (m *memoryFileInfo) Name() string       { return m.name }
func (m *memoryFileInfo) Size() int64        { return m.size }
func (m *memoryFileInfo) Mode() os.FileMode  { return m.mode }
func (m *memoryFileInfo) ModTime() time.Time { return m.modTime }
func (m *memoryFileInfo) IsDir() bool        { return m.isDir }
func (m *memoryFileInfo) Sys() any           { return nil }

// newMemoryFileInfo creates a new memoryFileInfo with faked properties.
func newMemoryFileInfo(name string, isDir bool, size int64) *memoryFileInfo {
	mode := os.FileMode(0644)
	if isDir {
		mode = os.FileMode(0755) | os.ModeDir
	}
	return &memoryFileInfo{
		name:    name,
		isDir:   isDir,
		size:    size,
		modTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		mode:    mode,
	}
}

// memoryDirEntry implements os.DirEntry for the in-memory file system.
type memoryDirEntry struct {
	info os.FileInfo
}

func (m memoryDirEntry) Name() string               { return m.info.Name() }
func (m memoryDirEntry) IsDir() bool                { return m.info.IsDir() }
func (m memoryDirEntry) Type() os.FileMode          { return m.info.Mode().Type() }
func (m memoryDirEntry) Info() (os.FileInfo, error) { return m.info, nil }

type memoryFileSystemImpl struct {
	files map[string]os.FileInfo
}

// newMemoryFileSystem creates a new memoryFileSystemImpl.
func newMemoryFileSystem() *memoryFileSystemImpl {
	return &memoryFileSystemImpl{
		files: make(map[string]os.FileInfo),
	}
}

// AddFile adds a file with the given os.FileInfo to the file system.
func (f *memoryFileSystemImpl) AddFile(path string, info os.FileInfo) {
	f.files[filepath.Clean(path)] = info
}

// AddRegularFile adds a regular file with faked metadata.
func (f *memoryFileSystemImpl) AddRegularFile(path string, size int64) {
	info := newMemoryFileInfo(filepath.Base(path), false, size)
	f.files[filepath.Clean(path)] = info
}

// AddDir adds a directory with faked metadata.
func (f *memoryFileSystemImpl) AddDir(path string) {
	info := newMemoryFileInfo(filepath.Base(path), true, 0)
	f.files[filepath.Clean(path)] = info
}

func (f *memoryFileSystemImpl) ReadDir(path string) ([]os.DirEntry, error) {
	entriesMap := make(map[string]os.DirEntry)
	cleanedPath := filepath.Clean(path)

	for p, info := range f.files {
		// Check if p is a direct child of cleanedPath
		parent := filepath.Dir(p)
		if parent == cleanedPath {
			entriesMap[info.Name()] = memoryDirEntry{info: info}
			continue
		}

		// Check for implicit directories
		if strings.HasPrefix(p, cleanedPath) {
			prefix := cleanedPath
			if prefix == "/" {
				// Special case for root, so we don't trim the leading / from p
				prefix = ""
			}

			relPath := strings.TrimPrefix(p, prefix)
			relPath = strings.TrimPrefix(relPath, string(filepath.Separator))

			parts := strings.Split(relPath, string(filepath.Separator))
			if len(parts) > 1 {
				childName := parts[0]
				if _, exists := entriesMap[childName]; !exists {
					dirInfo := newMemoryFileInfo(childName, true, 0)
					entriesMap[childName] = memoryDirEntry{info: dirInfo}
				}
			}
		}
	}

	var entries []os.DirEntry
	for _, entry := range entriesMap {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	return entries, nil
}

func (f *memoryFileSystemImpl) GetUnixMetadata(info os.FileInfo) (metadata unixMetadata, err error) {
	return unixMetadata{}, ErrMetadataNotSupported
}
