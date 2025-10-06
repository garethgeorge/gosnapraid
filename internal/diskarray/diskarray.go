package diskarray

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/garethgeorge/gosnapraid/internal/blockmap"
)

const snapraidDataDir = ".gosnapraid"

type DiskArray struct {
	BlockSize   int64
	DataDisks   []*DataDisk
	ParityDisks []*ParityDisk
}

type Disk interface {
	Size() int64
	BlockCount() int64
	ReadBlock(blockIndex int64) error
}

type DataDisk struct {
	BlockSize int64
	UID       string
	RootDir   string
	BlockMap  *blockmap.BlockMap
}

func NewDataDisk(rootDir string, blockSize int64, initialize bool) (*DataDisk, error) {
	// initialize optionally inits the disk if not initialized
	if initialize {
		if err := os.MkdirAll(rootDir, 0755); err != nil {
			return nil, fmt.Errorf("create data disk root dir: %w", err)
		}
		// Create .gosnapraid directory
		if err := os.MkdirAll(filepath.Join(rootDir, snapraidDataDir), 0755); err != nil {
			return nil, fmt.Errorf("create .gosnapraid dir: %w", err)
		}
	}

	// Get the metadata for the path
	metadata, err := getMetadataForPath(rootDir)
	if err != nil {
		return nil, fmt.Errorf("get data disk metadata: %w", err)
	}

	_, err = os.Stat(filepath.Join(rootDir, snapraidDataDir, "metadata.json"))
	if os.IsNotExist(err) {
		if !initialize {
			return nil, fmt.Errorf("data disk not initialized at path %s", rootDir)
		}
		writeMetadataFile(rootDir, metadata)
	} else if err != nil {
		return nil, fmt.Errorf("stat metadata file: %w", err)
	} else {
		// Metadata file exists, read and verify
		existingMetadata, err := readMetadataFile(rootDir)
		if err != nil {
			return nil, fmt.Errorf("read metadata file: %w", err)
		}
		if existingMetadata != metadata {
			return nil, fmt.Errorf("metadata mismatch for data disk at path %s, %+v != %+v", rootDir, existingMetadata, metadata)
		}
	}

	// Load or create the blockmap
	blockmap := blockmap.NewBlockMap(0, metadata.Size/blockSize-1) // BlockCount is size divided by block size minus 1 for inclusive indexing.
	blockmapFile := filepath.Join(rootDir, snapraidDataDir, "blockmap.dat")
	if f, err := os.Open(blockmapFile); errors.Is(err, os.ErrNotExist) {
		if !initialize {
			return nil, fmt.Errorf("data disk blockmap not found at path %s", blockmapFile)
		}

		// Create the file and save it empty
		fw, err := os.Create(blockmapFile)
		if err != nil {
			return nil, fmt.Errorf("create blockmap file: %w", err)
		}
		defer fw.Close()
		if err := blockmap.Serialize(fw); err != nil {
			return nil, fmt.Errorf("serialize new blockmap: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("stat blockmap file: %w", err)
	} else {
		defer f.Close()
		// Load the existing blockmap
		if err := blockmap.Deserialize(f); err != nil {
			return nil, fmt.Errorf("deserialize blockmap: %w", err)
		}
	}

	return &DataDisk{
		RootDir:  rootDir,
		BlockMap: blockmap,
	}, nil
}

type DiskBlockData struct {
	BlockIndex int64
	Data       []byte
	Error      error
	IsEmpty    bool
}

func (d *DataDisk) IterateBlocks() func(yield func(DiskBlockData) bool) {
	return func(yield func(DiskBlockData) bool) {
		emptyBuf := make([]byte, d.BlockSize)
		blockBuf := make([]byte, d.BlockSize)
		curIndex := int64(d.BlockMap.Domain.Start)
		endIndex := int64(d.BlockMap.Domain.End)

		for alloc := range d.BlockMap.AllocationIter() {
			start := alloc.Start
			end := alloc.End
			fpath := filepath.Join(d.RootDir, alloc.Name)

			// Provide empty blocks for each chunk prior to the current alocation
			for curIndex <= endIndex && curIndex < start {
				if yield(DiskBlockData{
					BlockIndex: curIndex,
					Data:       emptyBuf,
					IsEmpty:    true,
				}) {
					return
				}
			}

			// Read the block data for the current allocation
			f, err := os.Open(fpath)
			if err != nil {
				fmtErr := fmt.Errorf("open block file %s: %w", fpath, err)
				for curIndex <= endIndex && curIndex <= end {
					if yield(DiskBlockData{
						BlockIndex: curIndex,
						Data:       nil,
						Error:      fmtErr,
						IsEmpty:    true,
					}) {
						f.Close()
						return
					}
					curIndex++
				}
				continue
			}
			for curIndex <= endIndex && curIndex <= end {
				_, err := io.ReadFull(f, blockBuf)
				if err != nil {

					fmtErr := fmt.Errorf("read block file %s: %w", fpath, err)
					if yield(DiskBlockData{
						BlockIndex: curIndex,
						Data:       nil,
						Error:      fmtErr,
						IsEmpty:    true,
					}) {
						f.Close()
						return
					}
				} else {
					if yield(DiskBlockData{
						BlockIndex: curIndex,
						Data:       blockBuf,
						IsEmpty:    false,
					}) {
						f.Close()
						return
					}
				}
			}
			defer f.Close()

			// Yield the block data
			// if err := yield(curIndex, blockBuf); err != nil {
			// 	return
			// }
		}
	}
}

type ParityDisk struct {
	RootDir string
}

func (d *ParityDisk) WriteBlock(blockIndex int64) error {
	return nil
}
