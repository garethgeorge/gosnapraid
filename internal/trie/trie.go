package trie

import (
	"bytes"
	"sort"
	"strings"

	"github.com/garethgeorge/gosnapraid/internal/bumpalloc"
)

const (
	flagIsBtree  = 1 << iota
	flagHasValue = 1 << iota
)

type Trie[T any] struct {
	alloc     *bumpalloc.Allocator
	root      *node[T]
	separator rune

	tmpParents []*node[T] // reused slice for tracking parent nodes during insertions
}

func NewTrie[T any](sep rune, alloc *bumpalloc.Allocator) *Trie[T] {
	return &Trie[T]{root: &node[T]{}}
}

func (t *Trie[T]) splitPath(path string) [][]byte {
	var segments [][]byte
	for _, seg := range strings.Split(path, string(t.separator)) {
		segments = append(segments, []byte(seg))
	}
	return segments
}

func (t *Trie[T]) renderPath(segments [][]byte) string {
	var parts []string
	for _, seg := range segments {
		parts = append(parts, string(seg))
	}
	return strings.Join(parts, string(t.separator))
}

func (t *Trie[T]) Set(path string, value T) {
	segments := t.splitPath(path)

	t.tmpParents = t.tmpParents[:0]
	defer func() {
		for i := range t.tmpParents {
			t.tmpParents[i] = nil
		}
		t.tmpParents = t.tmpParents[:0]
	}()

	current := t.root
	for i := 0; i < len(segments); i++ {
		segment := segments[i]
		t.tmpParents = append(t.tmpParents, current)

		if len(current.children) == 0 {
			// No children, add new child and continue down
			newNode := node[T]{prefix: t.alloc.Alloc(segment)}
			current.children = append(current.children, newNode)
			current = &current.children[0]
			continue
		}

		// Binary search children for the segment
		idx, found := sort.Find(len(current.children), func(j int) int {
			buf := t.alloc.Get(current.children[j].prefix)
			return bytes.Compare(buf, segment)
		})

		if found {
			// Found existing child, continue down
			current = &current.children[idx]
		} else {
			// Not found, insert a new child at idx
			newNode := node[T]{prefix: t.alloc.Alloc(segment)}
			current.children = append(current.children, node[T]{}) // Append empty to grow slice
			copy(current.children[idx+1:], current.children[idx:])
			current.children[idx] = newNode
			current = &current.children[idx]
		}
	}

	// Set the value at the final node
	current.value = value
	current.flag |= flagHasValue
}

func (t *Trie[T]) Get(path string) T {
	var zero T
	segments := t.splitPath(path)

	current := t.root
	for i := 0; i < len(segments); i++ {
		segment := segments[i]

		// Binary search children for the segment
		idx, found := sort.Find(len(current.children), func(j int) int {
			buf := t.alloc.Get(current.children[j].prefix)
			return bytes.Compare(buf, segment)
		})

		if !found {
			return zero
		}
		current = &current.children[idx]
	}

	// Return the value if it exists
	if current.flag&flagHasValue != 0 {
		return current.value
	}
	return zero
}

type node[T any] struct {
	prefix   bumpalloc.Seg
	value    T
	children []node[T]
	flag     uint8
}
