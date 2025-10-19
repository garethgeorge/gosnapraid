package testutil

import (
	"fmt"
	"io/fs"
	"path"
	"testing/fstest"
	"time"
)

// TreeConfig defines the structure of a generated filesystem tree.
type TreeConfig struct {
	// Depth is the maximum depth of the directory tree (0 = root only).
	Depth int

	// BreadthPerDir is the number of subdirectories to create at each level.
	BreadthPerDir int

	// FilesPerDir is the number of files to create in each directory.
	FilesPerDir int

	// LeafFilesOnly, if true, only creates files at the maximum depth (leaf directories).
	// If false, creates files at every directory level.
	LeafFilesOnly bool

	// FileSize is the size of each generated file in bytes.
	FileSize int

	// ModTime is the modification time for all files and directories.
	// If zero, uses the current time.
	ModTime time.Time
}

// DefaultTreeConfig returns a reasonable default configuration for testing.
func DefaultTreeConfig() TreeConfig {
	return TreeConfig{
		Depth:         3,
		BreadthPerDir: 2,
		FilesPerDir:   3,
		LeafFilesOnly: false,
		FileSize:      1024,
		ModTime:       time.Unix(1234567890, 0),
	}
}

// GenerateMapFS creates a testing/fstest.MapFS with a tree structure
// defined by the given configuration.
func GenerateMapFS(config TreeConfig) fstest.MapFS {
	if config.ModTime.IsZero() {
		config.ModTime = time.Now()
	}

	mapFS := make(fstest.MapFS)
	generateTree(mapFS, "", 0, config)
	return mapFS
}

// generateTree recursively generates the directory tree.
func generateTree(mapFS fstest.MapFS, currentPath string, currentDepth int, config TreeConfig) {
	// Create files in the current directory if appropriate
	shouldCreateFiles := !config.LeafFilesOnly || currentDepth == config.Depth
	if shouldCreateFiles {
		for i := 0; i < config.FilesPerDir; i++ {
			fileName := fmt.Sprintf("file%d.txt", i)
			filePath := path.Join(currentPath, fileName)
			if currentPath == "" {
				filePath = fileName
			}

			mapFS[filePath] = &fstest.MapFile{
				Data:    makeFileContent(filePath, config.FileSize),
				Mode:    0644,
				ModTime: config.ModTime,
			}
		}
	}

	// If we haven't reached max depth, create subdirectories
	if currentDepth < config.Depth {
		for i := 0; i < config.BreadthPerDir; i++ {
			dirName := fmt.Sprintf("dir%d", i)
			dirPath := path.Join(currentPath, dirName)
			if currentPath == "" {
				dirPath = dirName
			}

			// Create the directory entry
			mapFS[dirPath] = &fstest.MapFile{
				Mode:    fs.ModeDir | 0755,
				ModTime: config.ModTime,
			}

			// Recursively generate subdirectories
			generateTree(mapFS, dirPath, currentDepth+1, config)
		}
	}
}

// makeFileContent generates file content of the specified size.
// The content is deterministic based on the file path for reproducibility.
func makeFileContent(filePath string, size int) []byte {
	if size == 0 {
		return []byte{}
	}

	content := make([]byte, size)
	pattern := []byte(fmt.Sprintf("Content for %s\n", filePath))

	// Fill the content with the pattern repeated
	for i := 0; i < size; i++ {
		content[i] = pattern[i%len(pattern)]
	}

	return content
}

// CountExpectedFiles calculates how many files will be generated with the given config.
func CountExpectedFiles(config TreeConfig) int {
	return countFilesAtDepth(0, config)
}

func countFilesAtDepth(depth int, config TreeConfig) int {
	// Calculate number of directories at this depth
	// At depth 0: 1 (root)
	// At depth 1: BreadthPerDir
	// At depth 2: BreadthPerDir^2
	// etc.
	var dirsAtDepth int
	if depth == 0 {
		dirsAtDepth = 1
	} else {
		dirsAtDepth = 1
		for i := 0; i < depth; i++ {
			dirsAtDepth *= config.BreadthPerDir
		}
	}

	// Count files at this depth
	var filesAtDepth int
	if config.LeafFilesOnly {
		if depth == config.Depth {
			filesAtDepth = dirsAtDepth * config.FilesPerDir
		}
	} else {
		filesAtDepth = dirsAtDepth * config.FilesPerDir
	}

	// If we haven't reached max depth, recurse
	if depth < config.Depth {
		filesAtDepth += countFilesAtDepth(depth+1, config)
	}

	return filesAtDepth
}

// CountExpectedDirs calculates how many directories will be generated with the given config.
func CountExpectedDirs(config TreeConfig) int {
	if config.Depth == 0 {
		return 1 // Just the root
	}

	total := 1 // Root directory
	for depth := 1; depth <= config.Depth; depth++ {
		dirsAtDepth := 1
		for i := 0; i < depth; i++ {
			dirsAtDepth *= config.BreadthPerDir
		}
		total += dirsAtDepth
	}

	return total
}

// CountExpectedEntries calculates the total number of entries (files + directories).
func CountExpectedEntries(config TreeConfig) int {
	return CountExpectedFiles(config) + CountExpectedDirs(config)
}
