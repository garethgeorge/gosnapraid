package trie

import (
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/bumpalloc"
)

// TestSetGetBasic tests basic Set and Get operations
func TestSetGetBasic(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Test setting and getting a single value
	trie.Set("a", "value_a")
	if got := trie.Get("a"); got != "value_a" {
		t.Errorf("Get(a) = %v, want %v", got, "value_a")
	}

	// Test setting and getting a nested path
	trie.Set("a/b", "value_ab")
	if got := trie.Get("a/b"); got != "value_ab" {
		t.Errorf("Get(a/b) = %v, want %v", got, "value_ab")
	}

	// Test that parent node still has its value
	if got := trie.Get("a"); got != "value_a" {
		t.Errorf("Get(a) after setting a/b = %v, want %v", got, "value_a")
	}
}

// TestGetNonExistent tests getting values that don't exist
func TestGetNonExistent(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Test getting from empty trie
	if got := trie.Get("nonexistent"); got != "" {
		t.Errorf("Get(nonexistent) from empty trie = %v, want empty string", got)
	}

	// Add some values
	trie.Set("a/b/c", "value")

	// Test getting non-existent paths
	testCases := []string{
		"x",
		"a/x",
		"a/b/x",
		"a/b/c/d",
	}

	for _, path := range testCases {
		if got := trie.Get(path); got != "" {
			t.Errorf("Get(%s) = %v, want empty string", path, got)
		}
	}
}

// TestGetParentWithoutValue tests getting a parent path that exists but has no value
func TestGetParentWithoutValue(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Set a nested path
	trie.Set("a/b/c", "value_c")

	// Get parent paths that don't have values set
	if got := trie.Get("a"); got != "" {
		t.Errorf("Get(a) = %v, want empty string", got)
	}
	if got := trie.Get("a/b"); got != "" {
		t.Errorf("Get(a/b) = %v, want empty string", got)
	}

	// The actual path should have the value
	if got := trie.Get("a/b/c"); got != "value_c" {
		t.Errorf("Get(a/b/c) = %v, want %v", got, "value_c")
	}
}

// TestSetOverwrite tests overwriting existing values
func TestSetOverwrite(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Set initial value
	trie.Set("a/b", "value1")
	if got := trie.Get("a/b"); got != "value1" {
		t.Errorf("Get(a/b) = %v, want %v", got, "value1")
	}

	// Overwrite with new value
	trie.Set("a/b", "value2")
	if got := trie.Get("a/b"); got != "value2" {
		t.Errorf("Get(a/b) after overwrite = %v, want %v", got, "value2")
	}
}

// TestSetMultiplePaths tests setting multiple paths
func TestSetMultiplePaths(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[int]('/', alloc)

	paths := map[string]int{
		"a":     1,
		"a/b":   2,
		"a/b/c": 3,
		"a/b/d": 4,
		"a/e":   5,
		"f":     6,
		"f/g/h": 7,
		"x/y/z": 8,
	}

	// Set all paths
	for path, value := range paths {
		trie.Set(path, value)
	}

	// Verify all paths
	for path, expected := range paths {
		if got := trie.Get(path); got != expected {
			t.Errorf("Get(%s) = %v, want %v", path, got, expected)
		}
	}
}

// TestSetGetEmptyPath tests handling of empty paths
func TestSetGetEmptyPath(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Set and get empty path
	trie.Set("", "root_value")
	if got := trie.Get(""); got != "root_value" {
		t.Errorf("Get() for empty path = %v, want %v", got, "root_value")
	}
}

// TestSetGetDifferentSeparator tests using a different separator
func TestSetGetDifferentSeparator(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('.', alloc)

	trie.Set("a.b.c", "value")
	if got := trie.Get("a.b.c"); got != "value" {
		t.Errorf("Get(a.b.c) with '.' separator = %v, want %v", got, "value")
	}

	// Ensure '/' is treated as part of the key, not a separator
	trie.Set("x/y", "slash_value")
	if got := trie.Get("x/y"); got != "slash_value" {
		t.Errorf("Get(x/y) with '.' separator = %v, want %v", got, "slash_value")
	}
}

// TestSetGetDifferentTypes tests with different value types
func TestSetGetDifferentTypes(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		alloc := bumpalloc.NewAllocator(1024)
		trie := NewTrie[int]('/', alloc)
		trie.Set("a", 42)
		if got := trie.Get("a"); got != 42 {
			t.Errorf("Get(a) = %v, want %v", got, 42)
		}
		// Non-existent should return zero value (0)
		if got := trie.Get("b"); got != 0 {
			t.Errorf("Get(b) = %v, want %v", got, 0)
		}
	})

	t.Run("bool", func(t *testing.T) {
		alloc := bumpalloc.NewAllocator(1024)
		trie := NewTrie[bool]('/', alloc)
		trie.Set("a", true)
		if got := trie.Get("a"); got != true {
			t.Errorf("Get(a) = %v, want %v", got, true)
		}
		// Non-existent should return zero value (false)
		if got := trie.Get("b"); got != false {
			t.Errorf("Get(b) = %v, want %v", got, false)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type MyStruct struct {
			Name  string
			Value int
		}
		alloc := bumpalloc.NewAllocator(1024)
		trie := NewTrie[MyStruct]('/', alloc)
		expected := MyStruct{Name: "test", Value: 123}
		trie.Set("a", expected)
		if got := trie.Get("a"); got != expected {
			t.Errorf("Get(a) = %v, want %v", got, expected)
		}
	})
}

// TestSetGetLargeTrie tests with a large number of entries to trigger btree splits
func TestSetGetLargeTrie(t *testing.T) {
	alloc := bumpalloc.NewAllocator(4096)
	trie := NewTrie[int]('/', alloc)

	// Set many values to trigger btree node splits
	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		path := ""
		// Create paths like "0", "1", ..., "999"
		path = string(rune('a'+(i%26))) + "/" + string(rune('a'+((i/26)%26))) + "/" + string(rune('0'+(i%10)))
		trie.Set(path, i)
	}

	// Verify all values
	for i := 0; i < numEntries; i++ {
		path := string(rune('a'+(i%26))) + "/" + string(rune('a'+((i/26)%26))) + "/" + string(rune('0'+(i%10)))
		if got := trie.Get(path); got != i {
			t.Errorf("Get(%s) = %v, want %v", path, got, i)
		}
	}
}

// TestSetGetSimilarPaths tests paths with common prefixes
func TestSetGetSimilarPaths(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	paths := map[string]string{
		"abc":     "value1",
		"abcd":    "value2",
		"abcde":   "value3",
		"ab":      "value4",
		"a":       "value5",
		"abc/def": "value6",
	}

	// Set all paths
	for path, value := range paths {
		trie.Set(path, value)
	}

	// Verify all paths
	for path, expected := range paths {
		if got := trie.Get(path); got != expected {
			t.Errorf("Get(%s) = %v, want %v", path, got, expected)
		}
	}
}

// TestSetGetWithSpecialCharacters tests paths with special characters
func TestSetGetWithSpecialCharacters(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	paths := map[string]string{
		"hello world":  "value1",
		"test@file":    "value2",
		"file-name":    "value3",
		"file_name":    "value4",
		"file.txt":     "value5",
		"path/to/file": "value6",
		"üñíçødé":      "value7",
	}

	// Set all paths
	for path, value := range paths {
		trie.Set(path, value)
	}

	// Verify all paths
	for path, expected := range paths {
		if got := trie.Get(path); got != expected {
			t.Errorf("Get(%s) = %v, want %v", path, got, expected)
		}
	}
}

// TestSetGetConsecutivePaths tests setting paths in sorted order
func TestSetGetConsecutivePaths(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Set paths in alphabetical order
	paths := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, path := range paths {
		trie.Set(path, "value_"+path)
	}

	// Verify all paths
	for _, path := range paths {
		expected := "value_" + path
		if got := trie.Get(path); got != expected {
			t.Errorf("Get(%s) = %v, want %v", path, got, expected)
		}
	}
}

// TestSetGetReverseOrder tests setting paths in reverse order
func TestSetGetReverseOrder(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie[string]('/', alloc)

	// Set paths in reverse alphabetical order
	paths := []string{"j", "i", "h", "g", "f", "e", "d", "c", "b", "a"}
	for _, path := range paths {
		trie.Set(path, "value_"+path)
	}

	// Verify all paths
	for _, path := range paths {
		expected := "value_" + path
		if got := trie.Get(path); got != expected {
			t.Errorf("Get(%s) = %v, want %v", path, got, expected)
		}
	}
}
