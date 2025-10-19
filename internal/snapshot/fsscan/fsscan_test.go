package fsscan

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestWalkDirectory_BasicStructure(t *testing.T) {
	// Create a temporary directory structure
	tmpDir := t.TempDir()

	// Create test structure:
	// tmpDir/
	//   file1.txt
	//   subdir/
	//     file2.txt
	//     file3.txt
	if err := os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create file1.txt: %v", err)
	}
	subdir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subdir, 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subdir, "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create file2.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(subdir, "file3.txt"), []byte("content3"), 0644); err != nil {
		t.Fatalf("failed to create file3.txt: %v", err)
	}

	// Walk the directory and collect results
	var results []FileMetadata
	for metadata := range WalkDirectory(tmpDir) {
		results = append(results, metadata)
	}

	// Verify we got all expected items (root, file1.txt, subdir, file2.txt, file3.txt)
	if len(results) != 5 {
		t.Errorf("expected 5 items, got %d", len(results))
		for i, r := range results {
			t.Logf("  [%d] %s (err: %v)", i, r.Path, r.Error)
		}
	}

	// Check that all paths are relative (not absolute)
	for _, r := range results {
		if filepath.IsAbs(r.Path) {
			t.Errorf("expected relative path, got absolute: %s", r.Path)
		}
	}

	// Check for no errors
	for _, r := range results {
		if r.Error != nil {
			t.Errorf("unexpected error for %s: %v", r.Path, r.Error)
		}
	}

	// Check that files have non-zero size and mtime
	for _, r := range results {
		if strings.HasSuffix(r.Path, ".txt") {
			if r.Size == 0 {
				t.Errorf("file %s has zero size", r.Path)
			}
			if r.Mtime == 0 {
				t.Errorf("file %s has zero mtime", r.Path)
			}
			if !r.Mode.IsRegular() {
				t.Errorf("file %s is not a regular file, mode: %v", r.Path, r.Mode)
			}
		}
	}

	// Check that directories have Mode.IsDir() true
	for _, r := range results {
		if r.Path == "." || r.Path == "subdir" {
			if !r.Mode.IsDir() {
				t.Errorf("directory %s does not have IsDir mode, mode: %v", r.Path, r.Mode)
			}
		}
	}
}

func TestWalkDirectory_NonExistentDirectory(t *testing.T) {
	// Walk a non-existent directory
	nonExistentDir := "/path/that/does/not/exist/xyz123"

	var results []FileMetadata
	for metadata := range WalkDirectory(nonExistentDir) {
		results = append(results, metadata)
	}

	// Should get at least one error result
	if len(results) == 0 {
		t.Fatal("expected at least one result with error")
	}

	// Check that we got an error
	hasError := false
	for _, r := range results {
		if r.Error != nil {
			hasError = true
			break
		}
	}

	if !hasError {
		t.Error("expected error result for non-existent directory")
	}
}

func TestWalkDirectory_EarlyCancellation(t *testing.T) {
	// Create a temporary directory with multiple files
	tmpDir := t.TempDir()

	for i := 0; i < 10; i++ {
		filename := filepath.Join(tmpDir, filepath.Base(tmpDir)+string(rune('a'+i))+".txt")
		if err := os.WriteFile(filename, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	// Walk the directory but stop after 3 items
	var results []FileMetadata
	count := 0
	for metadata := range WalkDirectory(tmpDir) {
		results = append(results, metadata)
		count++
		if count >= 3 {
			break // Early cancellation
		}
	}

	// Should have exactly 3 items
	if len(results) != 3 {
		t.Errorf("expected 3 items due to early cancellation, got %d", len(results))
	}

	// All results should be without error (we cancelled before any errors)
	for _, r := range results {
		if r.Error != nil {
			t.Errorf("unexpected error during early cancellation: %v", r.Error)
		}
	}
}

func TestWalkDirectory_YieldFalseStopsIteration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create several files
	for i := 0; i < 5; i++ {
		filename := filepath.Join(tmpDir, filepath.Base(tmpDir)+string(rune('a'+i))+".txt")
		if err := os.WriteFile(filename, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	// Use the iterator with yield returning false after first item
	count := 0
	for range WalkDirectory(tmpDir) {
		count++
		if count == 1 {
			break
		}
	}

	// Should have stopped after 1 item
	if count != 1 {
		t.Errorf("expected 1 item, got %d", count)
	}
}

func TestWalkDirectory_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	var results []FileMetadata
	for metadata := range WalkDirectory(tmpDir) {
		results = append(results, metadata)
	}

	// Should have exactly 1 result (the root directory itself)
	if len(results) != 1 {
		t.Errorf("expected 1 item for empty directory, got %d", len(results))
	}

	if len(results) > 0 {
		if results[0].Error != nil {
			t.Errorf("unexpected error for empty directory: %v", results[0].Error)
		}
		if !results[0].Mode.IsDir() {
			t.Errorf("root should be a directory, got mode: %v", results[0].Mode)
		}
	}
}

func TestWalkDirectory_PathFormat(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a simple file
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	var paths []string
	for metadata := range WalkDirectory(tmpDir) {
		paths = append(paths, metadata.Path)
	}

	// All paths should be relative (not absolute)
	for _, path := range paths {
		if filepath.IsAbs(path) {
			t.Errorf("path should be relative, got absolute: %s", path)
		}
	}

	// The root directory should be "."
	if len(paths) > 0 && paths[0] != "." {
		t.Errorf("expected root path to be '.', got %q", paths[0])
	}

	// Check that test.txt is found with relative path
	found := false
	for _, path := range paths {
		if path == "test.txt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("test.txt not found in paths")
	}
}

func TestWalkDirectory_FileMetadataFields(t *testing.T) {
	tmpDir := t.TempDir()

	testContent := []byte("hello world")
	testFile := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	var fileMetadata *FileMetadata
	for metadata := range WalkDirectory(tmpDir) {
		if strings.HasSuffix(metadata.Path, "test.txt") {
			m := metadata
			fileMetadata = &m
			break
		}
	}

	if fileMetadata == nil {
		t.Fatal("test.txt not found in results")
	}

	// Check Size
	if fileMetadata.Size != int64(len(testContent)) {
		t.Errorf("expected size %d, got %d", len(testContent), fileMetadata.Size)
	}

	// Check Mtime is set (non-zero)
	if fileMetadata.Mtime == 0 {
		t.Error("expected non-zero mtime")
	}

	// Check Mode
	if !fileMetadata.Mode.IsRegular() {
		t.Errorf("expected regular file mode, got %v", fileMetadata.Mode)
	}

	// Check Error is nil
	if fileMetadata.Error != nil {
		t.Errorf("expected nil error, got %v", fileMetadata.Error)
	}

	// Check Path is correct
	if !strings.HasSuffix(fileMetadata.Path, "test.txt") {
		t.Errorf("expected path to end with test.txt, got %s", fileMetadata.Path)
	}
}

func TestWalkDirectory_NestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested structure: tmpDir/a/b/c/file.txt
	deepPath := filepath.Join(tmpDir, "a", "b", "c")
	if err := os.MkdirAll(deepPath, 0755); err != nil {
		t.Fatalf("failed to create nested directories: %v", err)
	}

	deepFile := filepath.Join(deepPath, "file.txt")
	if err := os.WriteFile(deepFile, []byte("deep content"), 0644); err != nil {
		t.Fatalf("failed to create deep file: %v", err)
	}

	var results []FileMetadata
	for metadata := range WalkDirectory(tmpDir) {
		results = append(results, metadata)
	}

	// Should have: root, a/, a/b/, a/b/c/, a/b/c/file.txt = 5 items
	if len(results) != 5 {
		t.Errorf("expected 5 items for nested structure, got %d", len(results))
		for i, r := range results {
			t.Logf("  [%d] %s", i, r.Path)
		}
	}

	// Check that deep file is found
	found := slices.ContainsFunc(results, func(m FileMetadata) bool {
		return strings.HasSuffix(m.Path, "file.txt")
	})

	if !found {
		t.Error("deep file.txt not found in results")
	}
}
