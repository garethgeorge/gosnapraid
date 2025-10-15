package trie

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/bumpalloc"
)

// trieSort provides a sort order consistent with the trie's segment-based traversal.
func trieSort(paths []string, delim rune) {
	sort.Slice(paths, func(i, j int) bool {
		s1 := strings.Split(paths[i], string(delim))
		s2 := strings.Split(paths[j], string(delim))

		minLen := len(s1)
		if len(s2) < minLen {
			minLen = len(s2)
		}

		for k := 0; k < minLen; k++ {
			if s1[k] != s2[k] {
				return s1[k] < s2[k]
			}
		}
		return len(s1) < len(s2)
	})
}

// collectPaths is a helper to get all paths from the iterator for testing.
func collectPaths(t *Trie) []string {
	var paths []string
	for path := range t.ToPaths() {
		paths = append(paths, path)
	}
	return paths
}

func TestTrie_InsertAndToPaths_Simple(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie('/', alloc)

	paths := []string{
		"a/b/c",
		"a/b/d",
		"a/c",
	}
	for _, p := range paths {
		trie.Insert(p)
	}

	resultPaths := collectPaths(trie)
	trieSort(paths, '/')

	if !reflect.DeepEqual(paths, resultPaths) {
		t.Errorf("Expected paths %v, but got %v", paths, resultPaths)
	}
}

func TestTrie_InsertAndToPaths_Split(t *testing.T) {
	alloc := bumpalloc.NewAllocator(4096)
	trie := NewTrie('/', alloc)
	trie.maxLevelChildren = 4 // Lower max children to trigger split easily

	var paths []string
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("a/%d", i)
		paths = append(paths, path)
		trie.Insert(path)
	}
	paths = append(paths, "b/c")
	trie.Insert("b/c")

	resultPaths := collectPaths(trie)
	trieSort(paths, '/')

	if !reflect.DeepEqual(paths, resultPaths) {
		t.Errorf("Expected paths after split %v, but got %v", paths, resultPaths)
	}
}

func TestTrie_PrefixAndDuplicates(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie('/', alloc)

	trie.Insert("a/b")
	trie.Insert("a/b/c")
	trie.Insert("a/b") // duplicate

	expected := []string{"a/b", "a/b/c"}
	results := collectPaths(trie)
	trieSort(expected, '/')

	if !reflect.DeepEqual(expected, results) {
		t.Errorf("Expected paths %v, but got %v", expected, results)
	}
}

func TestTrie_EmptyAndRootPath(t *testing.T) {
	alloc := bumpalloc.NewAllocator(1024)
	trie := NewTrie('/', alloc)

	trie.Insert("") // path for root
	trie.Insert("a")

	expected := []string{"", "a"}
	results := collectPaths(trie)
	trieSort(expected, '/')

	if !reflect.DeepEqual(expected, results) {
		t.Errorf("Expected paths %v, but got %v", expected, results)
	}
}

func FuzzTrie(f *testing.F) {
	f.Add("a/b/c\na/b/d")
	f.Add("a\na/b\na/b/c")
	f.Add("")
	f.Add("a//b")
	f.Add("x/y\nx/y")
	f.Add("some/prefix/1\nsome/prefix/2\nsome/prefix/1/child")
	f.Add("a/b/c\na/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z")

	f.Fuzz(func(t *testing.T, input string) {
		alloc := bumpalloc.NewAllocator(16384)
		trie := NewTrie('/', alloc)

		pathMap := make(map[string]struct{})
		pathsToInsert := strings.Split(input, "\n")

		for _, p := range pathsToInsert {
			trie.Insert(p)
			pathMap[p] = struct{}{}
		}

		resultPaths := collectPaths(trie)

		expectedPaths := make([]string, 0, len(pathMap))
		for p := range pathMap {
			expectedPaths = append(expectedPaths, p)
		}
		trieSort(expectedPaths, '/')

		if !reflect.DeepEqual(expectedPaths, resultPaths) {
			t.Errorf("Input: %q\nExpected: %v\nGot:      %v", input, expectedPaths, resultPaths)
		}
	})
}

// --- Benchmarks ---

func generatePaths(numPaths int) []string {
	paths := make([]string, numPaths)
	for i := 0; i < numPaths; i++ {
		// Create somewhat realistic paths, e.g., "prefix/hash1/hash2"
		// Using modulo to create some overlap in prefixes
		paths[i] = fmt.Sprintf("some/prefix/%d/%d", i%100, i)
	}
	return paths
}

func BenchmarkInsert(b *testing.B) {
	benchmarkSizes := []int{100, 1000, 10000}

	for _, numPaths := range benchmarkSizes {
		b.Run(fmt.Sprintf("%d-paths", numPaths), func(b *testing.B) {
			paths := generatePaths(numPaths)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Allocate enough memory to avoid measuring allocator resizing.
				alloc := bumpalloc.NewAllocator(16 * 1024)
				trie := NewTrie('/', alloc)
				for _, p := range paths {
					trie.Insert(p)
				}
			}
		})
	}
}

func BenchmarkToPaths(b *testing.B) {
	benchmarkSizes := []int{100, 1000, 10000}

	for _, numPaths := range benchmarkSizes {
		b.Run(fmt.Sprintf("%d-paths", numPaths), func(b *testing.B) {
			paths := generatePaths(numPaths)
			alloc := bumpalloc.NewAllocator(16 * 1024)
			trie := NewTrie('/', alloc)
			for _, p := range paths {
				trie.Insert(p)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for range trie.ToPaths() {
					// consume the iterator
				}
			}
		})
	}
}
