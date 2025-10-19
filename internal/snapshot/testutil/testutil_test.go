package testutil

import (
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/snapshot/fsscan"
)

func TestGenerateMapFS_BasicStructure(t *testing.T) {
	config := TreeConfig{
		Depth:         2,
		BreadthPerDir: 2,
		FilesPerDir:   2,
		LeafFilesOnly: false,
		FileSize:      100,
	}

	mapFS := GenerateMapFS(config)

	// Count entries in the MapFS
	entries := 0
	for range mapFS {
		entries++
	}

	// Note: MapFS doesn't include the root "." entry explicitly
	// So we compare against files + dirs (excluding root)
	expected := CountExpectedEntries(config) - 1 // Exclude root from count
	if entries != expected {
		t.Errorf("expected %d entries in MapFS, got %d", expected, entries)
		for path := range mapFS {
			t.Logf("  - %s", path)
		}
	}
}

func TestGenerateMapFS_RootFilesOnly(t *testing.T) {
	config := TreeConfig{
		Depth:         0,
		BreadthPerDir: 0,
		FilesPerDir:   3,
		LeafFilesOnly: false,
		FileSize:      50,
	}

	mapFS := GenerateMapFS(config)

	// Should have 3 files at root
	if len(mapFS) != 3 {
		t.Errorf("expected 3 entries, got %d", len(mapFS))
	}

	// All should be files
	for path, entry := range mapFS {
		if entry.Mode.IsDir() {
			t.Errorf("expected file, got directory: %s", path)
		}
		if len(entry.Data) != 50 {
			t.Errorf("expected file size 50, got %d for %s", len(entry.Data), path)
		}
	}
}

func TestGenerateMapFS_LeafFilesOnly(t *testing.T) {
	config := TreeConfig{
		Depth:         2,
		BreadthPerDir: 2,
		FilesPerDir:   3,
		LeafFilesOnly: true,
		FileSize:      100,
	}

	mapFS := GenerateMapFS(config)

	// Count files and directories
	fileCount := 0
	dirCount := 0
	for _, entry := range mapFS {
		if entry.Mode.IsDir() {
			dirCount++
		} else {
			fileCount++
		}
	}

	// With LeafFilesOnly=true, files should only be at depth 2
	// Dirs: 2 (depth 1) + 4 (depth 2) = 6 (root not in MapFS)
	// Files: 4 dirs at depth 2 * 3 files = 12
	expectedDirs := CountExpectedDirs(config) - 1 // Exclude root
	expectedFiles := CountExpectedFiles(config)

	if dirCount != expectedDirs {
		t.Errorf("expected %d directories, got %d", expectedDirs, dirCount)
	}
	if fileCount != expectedFiles {
		t.Errorf("expected %d files, got %d", expectedFiles, fileCount)
	}
}

func TestGenerateMapFS_AllFilesAtAllLevels(t *testing.T) {
	config := TreeConfig{
		Depth:         2,
		BreadthPerDir: 2,
		FilesPerDir:   1,
		LeafFilesOnly: false,
		FileSize:      10,
	}

	mapFS := GenerateMapFS(config)

	// Count files
	fileCount := 0
	for _, entry := range mapFS {
		if !entry.Mode.IsDir() {
			fileCount++
		}
	}

	// With LeafFilesOnly=false, each directory should have files
	// Dirs at depth 0: 1, files: 1
	// Dirs at depth 1: 2, files: 2
	// Dirs at depth 2: 4, files: 4
	// Total files: 1 + 2 + 4 = 7
	expectedFiles := CountExpectedFiles(config)

	if fileCount != expectedFiles {
		t.Errorf("expected %d files, got %d", expectedFiles, fileCount)
	}
}

func TestCountExpectedFiles(t *testing.T) {
	tests := []struct {
		name     string
		config   TreeConfig
		expected int
	}{
		{
			name: "depth 0, 3 files",
			config: TreeConfig{
				Depth:         0,
				BreadthPerDir: 0,
				FilesPerDir:   3,
				LeafFilesOnly: false,
			},
			expected: 3,
		},
		{
			name: "depth 1, breadth 2, 2 files per dir, not leaf only",
			config: TreeConfig{
				Depth:         1,
				BreadthPerDir: 2,
				FilesPerDir:   2,
				LeafFilesOnly: false,
			},
			expected: 2 + 2*2, // root: 2, two subdirs: 2 each
		},
		{
			name: "depth 1, breadth 2, 2 files per dir, leaf only",
			config: TreeConfig{
				Depth:         1,
				BreadthPerDir: 2,
				FilesPerDir:   2,
				LeafFilesOnly: true,
			},
			expected: 2 * 2, // only in leaf dirs (depth 1)
		},
		{
			name: "depth 2, breadth 3, 1 file per dir, not leaf only",
			config: TreeConfig{
				Depth:         2,
				BreadthPerDir: 3,
				FilesPerDir:   1,
				LeafFilesOnly: false,
			},
			expected: 1 + 3 + 9, // root: 1, depth 1: 3, depth 2: 9
		},
		{
			name: "depth 2, breadth 3, 1 file per dir, leaf only",
			config: TreeConfig{
				Depth:         2,
				BreadthPerDir: 3,
				FilesPerDir:   1,
				LeafFilesOnly: true,
			},
			expected: 9, // only in leaf dirs (depth 2)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CountExpectedFiles(tt.config)
			if got != tt.expected {
				t.Errorf("CountExpectedFiles() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestCountExpectedDirs(t *testing.T) {
	tests := []struct {
		name     string
		config   TreeConfig
		expected int
	}{
		{
			name: "depth 0",
			config: TreeConfig{
				Depth:         0,
				BreadthPerDir: 2,
			},
			expected: 1, // just root
		},
		{
			name: "depth 1, breadth 2",
			config: TreeConfig{
				Depth:         1,
				BreadthPerDir: 2,
			},
			expected: 1 + 2, // root + 2 subdirs
		},
		{
			name: "depth 2, breadth 3",
			config: TreeConfig{
				Depth:         2,
				BreadthPerDir: 3,
			},
			expected: 1 + 3 + 9, // root + 3 at depth 1 + 9 at depth 2
		},
		{
			name: "depth 3, breadth 2",
			config: TreeConfig{
				Depth:         3,
				BreadthPerDir: 2,
			},
			expected: 1 + 2 + 4 + 8, // 1 + 2^1 + 2^2 + 2^3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CountExpectedDirs(tt.config)
			if got != tt.expected {
				t.Errorf("CountExpectedDirs() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestWalkFS_WithGeneratedMapFS(t *testing.T) {
	config := TreeConfig{
		Depth:         3,
		BreadthPerDir: 2,
		FilesPerDir:   2,
		LeafFilesOnly: false,
		FileSize:      100,
	}

	mapFS := GenerateMapFS(config)

	// Walk the generated filesystem
	var results []fsscan.FileMetadata
	for metadata := range fsscan.WalkFS(mapFS) {
		results = append(results, metadata)
	}

	// Should get all entries (files + dirs)
	expectedEntries := CountExpectedEntries(config)

	// Note: WalkFS yields one entry per file/dir in the walk
	// The root "." is always included
	if len(results) != expectedEntries {
		t.Errorf("expected %d walk results, got %d", expectedEntries, len(results))
	}

	// All should have no errors
	for i, r := range results {
		if r.Error != nil {
			t.Errorf("result %d (%s) has error: %v", i, r.Path, r.Error)
		}
	}
}

func TestGenerateMapFS_LargeTree(t *testing.T) {
	config := TreeConfig{
		Depth:         4,
		BreadthPerDir: 3,
		FilesPerDir:   5,
		LeafFilesOnly: true,
		FileSize:      1024,
	}

	mapFS := GenerateMapFS(config)

	// Verify counts
	fileCount := 0
	dirCount := 0
	for _, entry := range mapFS {
		if entry.Mode.IsDir() {
			dirCount++
		} else {
			fileCount++
		}
	}

	expectedFiles := CountExpectedFiles(config)
	expectedDirs := CountExpectedDirs(config) - 1 // Exclude root

	if fileCount != expectedFiles {
		t.Errorf("expected %d files, got %d", expectedFiles, fileCount)
	}
	if dirCount != expectedDirs {
		t.Errorf("expected %d directories, got %d", expectedDirs, dirCount)
	}
}

func TestGenerateMapFS_FileContent(t *testing.T) {
	config := TreeConfig{
		Depth:         1,
		BreadthPerDir: 1,
		FilesPerDir:   2,
		LeafFilesOnly: false,
		FileSize:      50,
	}

	mapFS := GenerateMapFS(config)

	// Check that files have content of the right size
	for path, entry := range mapFS {
		if !entry.Mode.IsDir() {
			if len(entry.Data) != 50 {
				t.Errorf("file %s has size %d, expected 50", path, len(entry.Data))
			}
			if len(entry.Data) > 0 && entry.Data[0] == 0 {
				t.Errorf("file %s has all-zero content", path)
			}
		}
	}
}

func TestDefaultTreeConfig(t *testing.T) {
	config := DefaultTreeConfig()

	// Verify reasonable defaults
	if config.Depth <= 0 {
		t.Error("default depth should be positive")
	}
	if config.BreadthPerDir <= 0 {
		t.Error("default breadth should be positive")
	}
	if config.FilesPerDir <= 0 {
		t.Error("default files per dir should be positive")
	}
	if config.FileSize <= 0 {
		t.Error("default file size should be positive")
	}

	// Should be able to generate a filesystem
	mapFS := GenerateMapFS(config)
	if len(mapFS) == 0 {
		t.Fatal("generated filesystem is empty")
	}
}

func TestGenerateMapFS_EmptyFileSize(t *testing.T) {
	config := TreeConfig{
		Depth:         1,
		BreadthPerDir: 1,
		FilesPerDir:   2,
		LeafFilesOnly: false,
		FileSize:      0, // Empty files
	}

	mapFS := GenerateMapFS(config)

	// Check that files exist but are empty
	fileCount := 0
	for path, entry := range mapFS {
		if !entry.Mode.IsDir() {
			fileCount++
			if len(entry.Data) != 0 {
				t.Errorf("file %s should be empty, got size %d", path, len(entry.Data))
			}
		}
	}

	if fileCount == 0 {
		t.Error("no files were created")
	}
}

func BenchmarkGenerateMapFS(b *testing.B) {
	config := TreeConfig{
		Depth:         3,
		BreadthPerDir: 3,
		FilesPerDir:   3,
		LeafFilesOnly: false,
		FileSize:      1024,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GenerateMapFS(config)
	}
}

func BenchmarkWalkFS_GeneratedTree(b *testing.B) {
	config := TreeConfig{
		Depth:         3,
		BreadthPerDir: 3,
		FilesPerDir:   3,
		LeafFilesOnly: false,
		FileSize:      1024,
	}

	mapFS := GenerateMapFS(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		for range fsscan.WalkFS(mapFS) {
			count++
		}
	}
}

// Example showing how to use GenerateMapFS with WalkFS for testing
func ExampleGenerateMapFS() {
	// Create a simple tree configuration
	config := TreeConfig{
		Depth:         2,
		BreadthPerDir: 2,
		FilesPerDir:   1,
		LeafFilesOnly: false,
		FileSize:      100,
	}

	// Generate the in-memory filesystem
	mapFS := GenerateMapFS(config)

	// Walk the filesystem
	fileCount := 0
	dirCount := 0
	for metadata := range fsscan.WalkFS(mapFS) {
		if metadata.Mode.IsDir() {
			dirCount++
		} else {
			fileCount++
		}
	}

	// The counts include the root directory
	println("Files:", fileCount)
	println("Directories:", dirCount)
}
