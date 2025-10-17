package trie

import (
	"bytes"
	"slices"
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

	maxBtreeChildren int
}

func NewTrie[T any](sep rune, alloc *bumpalloc.Allocator) *Trie[T] {
	return &Trie[T]{
		alloc:            alloc,
		root:             &node[T]{},
		separator:        sep,
		maxBtreeChildren: 32,
	}
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

func (t *Trie[T]) btreeInsert(current *node[T], key bumpalloc.Seg, keyBytes []byte) *node[T] {
	if keyBytes == nil {
		keyBytes = t.alloc.Get(key)
	}

	// Create a node at the correct position in current for segment and return the refernce to it
	idx := sort.Search(len(current.children), func(j int) bool {
		buf := t.alloc.Get(current.children[j].prefix)
		return bytes.Compare(buf, keyBytes) >= 0
	})
	newNode := node[T]{prefix: key}
	if idx >= len(current.children) {
		current.children = append(current.children, newNode)
		return &current.children[len(current.children)-1]
	} else {
		current.children = append(current.children, node[T]{})
		copy(current.children[idx+1:], current.children[idx:])
		current.children[idx] = newNode
		return &current.children[idx]
	}
}

// BtreeCreate creates a new node in a btree structure with the key 'segment' and returns either of
// 1) the newly created node
// 2) optionally a node that was split off if a split occurred, the caller must insert this into the parent.
func (t *Trie[T]) btreeCreateHelper(current *node[T], segment []byte) (created *node[T], split *node[T]) {
	if current.flag&flagIsBtree == 0 {
		// We're in an interior node, determine which child we should insert into
		idx, _ := sort.Find(len(current.children), func(j int) int {
			buf := t.alloc.Get(current.children[j].prefix)
			return bytes.Compare(buf, segment)
		})
		child := &current.children[idx]

		childCreated, childSplit := t.btreeCreateHelper(child, segment)
		created = childCreated

		if childSplit != nil {
			if len(current.children) >= t.maxBtreeChildren {
				// Split the current node
				midIdx := len(current.children) / 2
				split = current.split(midIdx)
				if bytes.Compare(t.alloc.Get(split.prefix), segment) <= 0 {
					// Insert into the new split node
					current = split
				}

				// Since we just expanded, we don't know where to insert the split child, re-search
				*current.insertChild(t.alloc, childSplit.prefix, t.alloc.Get(childSplit.prefix)) = *childSplit
			} else {
				// Insert just after the child that was split
				*current.insertChildAt(idx+1, childSplit.prefix) = *childSplit
			}
		}
	} else {
		if len(current.children) >= t.maxBtreeChildren {
			// Split the current node
			midIdx := len(current.children) / 2
			split = current.split(midIdx)
			if bytes.Compare(t.alloc.Get(split.prefix), segment) <= 0 {
				// Insert into the new split node
				current = split
			}
		}

		// We're in a btree leaf node, insert the new child here
		seg := t.alloc.Alloc(segment)
		created = t.btreeInsert(current, seg, segment)
	}
	return created, split
}

func (t *Trie[T]) btreeCreate(segment []byte) *node[T] {
	created, split := t.btreeCreateHelper(t.root, segment)
	if split != nil {
		// Need to create a new root
		newRoot := &node[T]{flag: flagIsBtree}
		newRoot.children = append(newRoot.children, *t.root)
		newRoot.children = append(newRoot.children, *split)
		t.root = newRoot
	}

	return created
}

func (t *Trie[T]) btreeSearch(root *node[T], segment []byte) *node[T] {
	current := root

	for current.flag&flagIsBtree != 0 {
		idx, _ := sort.Find(len(current.children), func(j int) int {
			buf := t.alloc.Get(current.children[j].prefix)
			return bytes.Compare(buf, segment)
		})
		current = &current.children[idx]
	}

	idx, found := sort.Find(len(current.children), func(j int) int {
		buf := t.alloc.Get(current.children[j].prefix)
		return bytes.Compare(buf, segment)
	})
	if !found {
		return nil
	}

	return &current.children[idx]
}

func (t *Trie[T]) Set(path string, value T) {
	segments := t.splitPath(path)

	current := t.root
	for i := 0; i < len(segments); i++ {
		segment := segments[i]

		if len(current.children) == 0 {
			// No children, add new child and continue down
			newNode := node[T]{prefix: t.alloc.Alloc(segment)}
			current.children = append(current.children, newNode)
			current = &current.children[0]
			continue
		}

		// TODO: optimize for the non-btree case where we can do this in one pass without searching twice
		// Binary search children for the segment
		child := t.btreeSearch(current, segment)
		if child == nil {
			child = t.btreeCreate(segment)
		}
		current = child
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

		child := t.btreeSearch(current, segment)
		if child == nil {
			return zero
		}
		current = child
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

func (n *node[T]) insertChild(alloc *bumpalloc.Allocator, key bumpalloc.Seg, keyBytes []byte) *node[T] {
	idx := sort.Search(len(n.children), func(i int) bool {
		buf := alloc.Get(n.children[i].prefix)
		return bytes.Compare(buf, keyBytes) >= 0
	})
	return n.insertChildAt(idx, key)
}

func (n *node[T]) insertChildAt(at int, key bumpalloc.Seg) *node[T] {
	n.children = append(n.children, node[T]{})
	copy(n.children[at+1:], n.children[at:])
	n.children[at] = node[T]{prefix: key}
	return &n.children[at]
}

func (n *node[T]) split(at int) *node[T] {
	newNode := &node[T]{
		prefix:   n.children[at].prefix,
		children: append([]node[T]{}, n.children[at:]...),
		flag:     n.flag,
	}
	n.children = slices.Clone(n.children[:at])
	return newNode
}
