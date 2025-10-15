package trie

import (
	"bytes"
	"math"
	"sort"
	"strings"

	"github.com/garethgeorge/gosnapraid/internal/bumpalloc"
)

const (
	FlagIndexNode = 1 << iota
	FlagPathTerminus
	DefaultMaxChildren = 32
)

// Path is a sequence of segments that represents a path in the trie.
type Path []bumpalloc.Seg

// Trie is a memory-efficient trie implementation.
type Trie struct {
	delim            rune
	maxLevelChildren int
	alloc            *bumpalloc.Allocator
	root             node
}

// node represents a node in the trie.
type node struct {
	segment  bumpalloc.Seg
	children []node
	flags    uint8
}

// NewTrie creates a new Trie.
func NewTrie(delim rune, alloc *bumpalloc.Allocator) *Trie {
	return &Trie{
		delim:            delim,
		maxLevelChildren: DefaultMaxChildren,
		alloc:            alloc,
		root:             node{},
	}
}

// Insert adds a path to the trie.
func (t *Trie) Insert(path string) {
	if path == "" {
		t.root.flags |= FlagPathTerminus
		return
	}
	segments := strings.Split(path, string(t.delim))
	byteSegments := make([][]byte, len(segments))
	for i, s := range segments {
		byteSegments[i] = []byte(s)
	}
	t.root.insert(byteSegments, t)
}

// insert is a recursive helper to insert path segments.
func (n *node) insert(segments [][]byte, t *Trie) {
	if len(segments) == 0 {
		n.flags |= FlagPathTerminus
		return
	}
	child := n.getOrCreateChild(segments[0], t)
	child.insert(segments[1:], t)
}

// ToPaths returns an iterator function that can be used with a for-range loop (Go 1.22+).
// It yields all paths in the trie in sorted order.
func (t *Trie) ToPaths() func(yield func(string) bool) {
	return func(yield func(string) bool) {
		var traverse func(*node, string) bool

		traverse = func(n *node, prefix string) bool {
			if n.flags&FlagPathTerminus != 0 {
				if !yield(prefix) {
					return false
				}
			}

			if n.flags&FlagIndexNode != 0 {
				// Index node. Its children are buckets. Recursively call on them.
				// The prefix does not change when descending into a bucket.
				for i := range n.children {
					if !traverse(&n.children[i], prefix) {
						return false
					}
				}
			} else {
				// Simple node. Its children are trie nodes.
				for i := range n.children {
					child := &n.children[i]
					segBytes := t.alloc.Get(child.segment)

					var newPrefix string
					// Special-case the root's children to not prepend a delimiter.
					if n == &t.root {
						newPrefix = string(segBytes)
					} else {
						newPrefix = prefix + string(t.delim) + string(segBytes)
					}

					if !traverse(child, newPrefix) {
						return false
					}
				}
			}
			return true
		}

		traverse(&t.root, "")
	}
}

// getOrCreateChild finds a child for a segment, creating it if it doesn't exist.
// It handles node splitting.
func (n *node) getOrCreateChild(segment []byte, t *Trie) *node {
	// If this is an index node, find the right bucket and recurse.
	if n.flags&FlagIndexNode != 0 {
		bucketIndex := sort.Search(len(n.children), func(i int) bool {
			pivotSeg := t.alloc.Get(n.children[i].segment)
			return bytes.Compare(pivotSeg, segment) > 0
		})
		if bucketIndex > 0 {
			bucketIndex--
		}
		return n.children[bucketIndex].getOrCreateChild(segment, t)
	}

	// This is a simple node. Search for the child.
	index := sort.Search(len(n.children), func(i int) bool {
		childSeg := t.alloc.Get(n.children[i].segment)
		return bytes.Compare(childSeg, segment) >= 0
	})

	// If found, return it.
	if index < len(n.children) {
		childSeg := t.alloc.Get(n.children[index].segment)
		if bytes.Equal(childSeg, segment) {
			return &n.children[index]
		}
	}

	// Not found. Create and insert new child.
	newSeg := t.alloc.Alloc(segment)
	newNode := node{segment: newSeg}

	// Insert into sorted children slice.
	n.children = append(n.children, node{})
	copy(n.children[index+1:], n.children[index:])
	n.children[index] = newNode

	// If node is now too big, split it.
	if len(n.children) > t.maxLevelChildren {
		n.split(t)
		// After split, the new node is in a bucket. We need to find it again.
		return n.findChild(segment, t)
	}

	return &n.children[index]
}

// findChild finds a child node for a given segment. It's used to re-find a
// node after a split.
func (n *node) findChild(segment []byte, t *Trie) *node {
	if n.flags&FlagIndexNode != 0 {
		// Index node, find the bucket.
		bucketIndex := sort.Search(len(n.children), func(i int) bool {
			pivotSeg := t.alloc.Get(n.children[i].segment)
			return bytes.Compare(pivotSeg, segment) > 0
		})
		if bucketIndex > 0 {
			bucketIndex--
		}
		return n.children[bucketIndex].findChild(segment, t)
	}

	// Simple node, binary search.
	index := sort.Search(len(n.children), func(i int) bool {
		childSeg := t.alloc.Get(n.children[i].segment)
		return bytes.Compare(childSeg, segment) >= 0
	})

	if index < len(n.children) {
		childSeg := t.alloc.Get(n.children[index].segment)
		if bytes.Equal(childSeg, segment) {
			return &n.children[index]
		}
	}
	return nil // Should not happen if we just inserted it.
}

// split converts a simple node into an index node with buckets.
func (n *node) split(t *Trie) {
	childrenToSplit := n.children

	numChildren := len(childrenToSplit)
	// Determine number of buckets, e.g., sqrt(N)
	numBuckets := int(math.Ceil(math.Sqrt(float64(numChildren))))
	if numBuckets < 2 {
		numBuckets = 2
	}
	bucketSize := (numChildren + numBuckets - 1) / numBuckets // ceiling division

	newBuckets := make([]node, 0, numBuckets)

	for i := 0; i < numChildren; i += bucketSize {
		end := i + bucketSize
		if end > numChildren {
			end = numChildren
		}

		bucketChildren := childrenToSplit[i:end]

		// Create a new bucket node.
		// The segment for the bucket is the segment of its first child, which acts as the pivot.
		newBucket := node{
			segment:  bucketChildren[0].segment,
			children: make([]node, len(bucketChildren)),
		}
		copy(newBucket.children, bucketChildren)
		newBuckets = append(newBuckets, newBucket)
	}

	// The current node becomes an index node.
	n.flags |= FlagIndexNode
	n.children = newBuckets
}
