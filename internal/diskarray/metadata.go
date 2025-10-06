package diskarray

import (
	"encoding/json"
	"os"

	"golang.org/x/sys/unix"
)

type DiskMetadata struct {
	RootDir    string `json:"rootDir"`
	RootDirIno uint64 `json:"rootDirIno"`
	RootDirGen uint32 `json:"rootDirGen"`
	Size       int64  `json:"sizeBytes"`
}

type ArrayMetadata struct {
	BlockSize   int64          `json:"blockSizeBytes"`
	DataDisks   []DiskMetadata `json:"dataDisks"`
	ParityDisks []DiskMetadata `json:"parityDisks"`
}

func getMetadataForPath(path string) (DiskMetadata, error) {
	// Poll the filesystem info for the path
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return DiskMetadata{}, err
	}

	// Get filesystem info
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)

	// Get the inode for the path
	var fileStat unix.Stat_t
	if err := unix.Stat(path, &fileStat); err != nil {
		return DiskMetadata{}, err
	}

	return DiskMetadata{
		RootDir:    path,
		RootDirIno: fileStat.Ino,
		RootDirGen: fileStat.Gen,
		Size:       totalBytes,
	}, nil
}

func writeMetadataFile(path string, metadata DiskMetadata) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(metadata); err != nil {
		return err
	}
	return nil
}

func readMetadataFile(path string) (DiskMetadata, error) {
	f, err := os.Open(path)
	if err != nil {
		return DiskMetadata{}, err
	}
	defer f.Close()
	var metadata DiskMetadata
	if err := json.NewDecoder(f).Decode(&metadata); err != nil {
		return DiskMetadata{}, err
	}
	return metadata, nil
}
