package snaparray

import (
	"os"

	"github.com/garethgeorge/gosnapraid/internal/snapshot"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
	"github.com/klauspost/compress/zstd"
)

type compressedSnapshotFile struct {
	Path   string
	Header *gosnapraidpb.SnapshotHeader
	Reader *snapshot.SnapshotReader

	file       *os.File
	zstdReader *zstd.Decoder
}

func openCompressedSnapshotFile(path string) (*compressedSnapshotFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	zstdReader, err := zstd.NewReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	reader, header, err := snapshot.NewSnapshotReader(zstdReader)
	if err != nil {
		zstdReader.Close()
		file.Close()
		return nil, err
	}

	return &compressedSnapshotFile{
		Path:       path,
		Reader:     reader,
		Header:     header,
		file:       file,
		zstdReader: zstdReader,
	}, nil
}

func (csf *compressedSnapshotFile) Close() error {
	csf.zstdReader.Close()
	if err := csf.file.Close(); err != nil {
		return err
	}
	return nil
}
