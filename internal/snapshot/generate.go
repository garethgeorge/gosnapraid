package snapshot

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/garethgeorge/gosnapraid/internal/errors"
	"github.com/garethgeorge/gosnapraid/internal/hashing"
)

type GenerateStats struct {
	NumDirsProcessed             atomic.Int64
	NumFilesProcessed            atomic.Int64
	NumOldSnapshotEntriesSkipped atomic.Int64
	NumNewSnapshotEntriesAdded   atomic.Int64
	NumSnapshotEntriesUnchanged  atomic.Int64
}

func GenerateSnapshotWalk(rootDir string, oldSnapshotReader SnapshotReader, newSnapshotWriter SnapshotWriter, stats *GenerateStats) error {
	var helper func(relativePath string) error
	helper = func(relativePath string) error {
		fullPath := rootDir + string(os.PathSeparator) + relativePath

		entries, err := os.ReadDir(fullPath)
		if err != nil {
			return err
		}

		// Sort the entries by name
		slices.SortFunc(entries, func(a, b os.DirEntry) int {
			return strings.Compare(a.Name(), b.Name())
		})

		// Check if we're ahead of the path tracked by the snapshot reader
		oldSnapshotRelPath := metadataParentsToPath(oldSnapshotReader.Parents())
		for strings.Compare(oldSnapshotRelPath, relativePath) < 0 {
			if err := oldSnapshotReader.NextDirectory(); err != nil {
				if err == ErrNoMoreNodes {
					break
				}
				return err
			}
			oldSnapshotRelPath = metadataParentsToPath(oldSnapshotReader.Parents())
		}

		// Now process the entries in a zip-wise fashion
		i := 0
		j := 0
		for i < len(entries) && j < len(oldSnapshotReader.Siblings()) {
			if entry.IsDir() {
				stats.NumDirsProcessed.Add(1)
				if err := helper(relativePath + string(os.PathSeparator) + entry.Name()); err != nil {
					return err
				}
			} else {
				stats.NumFilesProcessed.Add(1)
			}
		}
		return nil
	}

	if err := helper(""); err != nil {
		return err
	}
	return nil
}

func GenerateSnapshot(realFilesystemIter FileSystemIterator, oldSnapshotReader SnapshotReader, newSnapshotWriter SnapshotWriter, stats *GenerateStats) error {
	// Read the header from the old snapshot
	if err := oldSnapshotReader.ReadHeader(); err != nil {
		return err
	}

	// Advance to the first snapshot directory, ignore the error if it's ErrNoMoreNodes
	oldSnapshotEntriesExhausted := false
	if err := oldSnapshotReader.NextDirectory(); err == ErrNoMoreNodes {
		oldSnapshotEntriesExhausted = true
	} else if err != nil {
		return err
	}

	var errAgg errors.ErrorAggregation
	for {
		// Advance to the next directory
		if err := realFilesystemIter.NextDirectory(); err == ErrNoMoreNodes {
			break
		} else if err != nil {
			return err
		}

		// Check if this iterator is synchronized with the directory pointed to by oldSnapshotReader
		curRelPath := realFilesystemIter.CurrentDirRelativePath()
		oldSnapshotRelPath := metadataParentsToPath(oldSnapshotReader.Parents())
		for !oldSnapshotEntriesExhausted && cmpDirPath(curRelPath, oldSnapshotRelPath) < 0 {
			if err := oldSnapshotReader.NextDirectory(); err != nil {
				if err == ErrNoMoreNodes {
					oldSnapshotEntriesExhausted = true
					continue
				}
				return err
			}
			oldSnapshotRelPath = metadataParentsToPath(oldSnapshotReader.Parents())
		}

		// If we're synchronized, then we should actually diff against the contents of the old directory
		oldSnapshotMetadata := []SnapshotNodeMetadata{}
		if cmpDirPath(curRelPath, oldSnapshotRelPath) == 0 {
			oldSnapshotMetadata = oldSnapshotReader.Siblings()
		}

		// Now we create directory nodes for the current directory
		curDirNodes := make([]SnapshotNodeMetadata, 0, len(realFilesystemIter.Siblings()))
		for _, entry := range realFilesystemIter.Siblings() {
			metadata, err := PartialSnapshotNodeMetadataFromDirEntry(entry)
			if err != nil {
				errAgg.Add(errors.NewFixedCause("get directory metadata", fmt.Errorf("for file %s/%s: %w", curRelPath, entry.Name(), err)))
				continue
			}
			curDirNodes = append(curDirNodes, metadata)
		}

		// Sort the nodes by partial fields (ino, gen, size, mode, modtime)
		slices.SortFunc(curDirNodes, func(a, b SnapshotNodeMetadata) int {
			return a.ComparePartialFields(&b)
		})

		// Sort the old snapshot metadata by partial fields (ino, gen, size, mode, modtime)
		slices.SortFunc(oldSnapshotMetadata, func(a, b SnapshotNodeMetadata) int {
			return a.ComparePartialFields(&b)
		})

		// First attempt to join the two lists to reuse hashes from the previous snapshot
		// Hash calculation is expensive, so we try to reuse as many hashes as possible
		i := 0
		j := 0
		resultDirNodes := []SnapshotNodeMetadata{}
		for i < len(curDirNodes) && j < len(oldSnapshotMetadata) {
			curNode := &curDirNodes[i]
			oldNode := &oldSnapshotMetadata[j]
			cmp := curNode.ComparePartialFields(oldNode)
			if cmp < 0 {
				// New entry
				stats.NumNewSnapshotEntriesAdded.Add(1)
				hash, err := computeHashForPath(curRelPath + string(os.PathSeparator) + curNode.Name)
				if err != nil {
					errAgg.Add(errors.NewFixedCause("compute hash", fmt.Errorf("for file %s/%s: %w", curRelPath, curNode.Name, err)))
					continue
				}
				curNode.ContentHash = hash
				resultDirNodes = append(resultDirNodes, *curNode)
				i++
			} else if cmp > 0 {
				// Old entry
				stats.NumOldSnapshotEntriesSkipped.Add(1)
				j++
			} else {
				// Match
				stats.NumSnapshotEntriesUnchanged.Add(1)
				resultDirNodes = append(resultDirNodes, *curNode)
				i++
				j++
			}
		}

		// Compute hashes for any remaining entries.
		for ; i < len(curDirNodes); i++ {
			curNode := &curDirNodes[i]
			hash, err := computeHashForPath(curRelPath + string(os.PathSeparator) + curNode.Name)
			if err != nil {
				errAgg.Add(errors.NewFixedCause("compute hash", fmt.Errorf("for file %s/%s: %w", curRelPath, curNode.Name, err)))
				continue
			}
			curNode.ContentHash = hash
			resultDirNodes = append(resultDirNodes, *curNode)
		}

		// Great, resultDirNodes now is a list of nodes that are fully qualified including hashes. Write them to the new snapshot.
		// Hmm, this depth first traversal nonsense isn't going work for me actually. That kind of sucks.

	}

	return nil
}

// cmpDirPath compares two directory paths, returning a negative number if a < b, 0 if a == b, and a positive number if a > b.
func cmpDirPath(a, b string) int {
	numASeps := strings.Count(a, string(os.PathSeparator))
	numBSeps := strings.Count(b, string(os.PathSeparator))
	if numASeps != numBSeps {
		return numASeps - numBSeps
	}
	return strings.Compare(a, b)
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
