package snapshot

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/garethgeorge/gosnapraid/internal/errors"
	"github.com/garethgeorge/gosnapraid/internal/hashing"
	"github.com/garethgeorge/gosnapraid/internal/sliceutil"
)

const defaultReadBufferSize = 256 * 1024

func GenerateSnapshot(rootDir string, oldSnapshotReader SnapshotReader, newSnapshotWriter SnapshotWriter) error {
	var errAgg errors.ErrorAggregation

	if err := newSnapshotWriter.WriteHeader(); err != nil {
		return fmt.Errorf("writing new snapshot header: %w", err)
	}
	if err := oldSnapshotReader.ReadHeader(); err != nil {
		return fmt.Errorf("reading old snapshot header: %w", err)
	}

	var helper func(relativePath string) error
	helper = func(relativePath string) error {
		fullPath := rootDir
		if relativePath != "" {
			fullPath = fullPath + string(os.PathSeparator) + relativePath
		}

		// Check if we're ahead of the path we've read upto in the old snapshot
		oldSnapshotRelPath := metadataParentsToPath(oldSnapshotReader.Parents())
		for strings.Compare(oldSnapshotRelPath, relativePath) < 0 {
			if err := oldSnapshotReader.NextDirectory(); err != nil {
				if err == ErrNoMoreNodes {
					break
				}
				return fmt.Errorf("advancing old snapshot reader: %w", err)
			}
			oldSnapshotRelPath = metadataParentsToPath(oldSnapshotReader.Parents())
		}

		// Read the directory entries for the current directory
		entries, err := os.ReadDir(fullPath)
		if err != nil {
			errAgg.Add(errors.NewFixedCause("read directory", fmt.Errorf("for directory %s: %w", fullPath, err)))
			return nil
		}
		slices.SortFunc(entries, func(a, b os.DirEntry) int {
			return strings.Compare(a.Name(), b.Name())
		})

		newSnapshotWriter.BeginChildren()
		iter := sliceutil.FullOuterJoinSlicesIter(entries, oldSnapshotReader.Siblings(), func(a os.DirEntry, b SnapshotNodeMetadata) int {
			return strings.Compare(a.Name(), b.Name)
		})
		for a, b := range iter {
			if a == nil {
				continue // skip over entries only in the old snapshot
			}
			newMetadata, err := SnapshotNodeMetadataFromDirEntry(a)
			if err != nil {
				errAgg.Add(errors.NewFixedCause("get directory metadata", fmt.Errorf("for file %s/%s: %w", relativePath, a.Name(), err)))
				continue
			}
			if b.Name != "" {
				// If there's a match, copy over the hash from the match.
				newMetadata.ContentHash = b.ContentHash
			} else {
				// If there's no match, compute the hash.
				if !a.IsDir() {
					hash, err := computeHashForPath(fullPath + string(os.PathSeparator) + a.Name())
					if err != nil {
						errAgg.Add(errors.NewFixedCause("compute hash", fmt.Errorf("for file %s/%s: %w", relativePath, a.Name(), err)))
						continue
					}
					newMetadata.ContentHash = hash
				} else {
					newMetadata.ContentHash = hashing.ContentHash{}
				}
			}
			if err := newSnapshotWriter.WriteNode(newMetadata); err != nil {
				return fmt.Errorf("writing new snapshot node: %w", err)
			}
			if a.IsDir() {
				if err := helper(relativePath + string(os.PathSeparator) + a.Name()); err != nil {
					return err
				}
			}
		}

		newSnapshotWriter.EndChildren()
		return nil
	}

	if err := helper(""); err != nil {
		return err
	}

	if errAgg.HasErrors() {
		return &errAgg
	}
	return nil
}

func metadataParentsToPath(parents []SnapshotNodeMetadata) string {
	var sb strings.Builder
	for _, parent := range parents {
		sb.WriteString(parent.Name)
		sb.WriteString(string(os.PathSeparator))
	}
	return sb.String()
}

func computeHashForPath(path string) (hashing.ContentHash, error) {
	// Open the file in read mode
	file, err := os.Open(path)
	if err != nil {
		return hashing.ContentHash{}, err
	}
	defer file.Close()

	// Create a buffered reader and use it to issue IO when computing the hash
	reader := bufio.NewReaderSize(file, defaultReadBufferSize)
	hash, err := hashing.ContentHashFromReader(hashing.CONTENT_HASH_ALGORITHM_XXH3, reader)
	if err != nil {
		return hashing.ContentHash{}, err
	}
	return hash, nil
}
