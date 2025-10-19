package snaparray

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/garethgeorge/gosnapraid/internal/snapshot"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
)

// Disk represents a single disk in the SnapRAID array.
type Disk struct {
	ID string
	FS fs.FS
}

func (d *Disk) snapshotPath(prefix string) string {
	return filepath.Join(prefix, "snapshot_"+d.ID+".snap.zstd")
}

// UpdateSnapshots updates the snapshots for the disk across all snapshot directories
// where the content manifest should be stored.
// Returns a function that can be used to finalize the snapshot update (e.g., renaming temp files).
func (d *Disk) updateSnapshotsHelper(dirs []string) (func() error, error) {
	// Create pipes for each snapshot writer
	var eg errgroup.Group
	var writers []io.Writer
	var closers []io.Closer

	for _, dir := range dirs {
		path := d.snapshotPath(dir) + ".tmp"
		piper, pipew := io.Pipe()
		writers = append(writers, pipew)
		closers = append(closers, pipew)

		f, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("create snapshot file: %w", err)
		}
		eg.Go(func() error {
			defer piper.Close()
			defer f.Close()
			if _, err := io.Copy(f, piper); err != nil {
				fmt.Printf("error writing snapshot to %s: %v\n", path, err)
			}
			return nil
		})
	}

	multiwriter := io.MultiWriter(writers...)
	zstdWriter, err := zstd.NewWriter(multiwriter,
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(4),
		zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("create zstd writer: %w", err)
	}
	defer zstdWriter.Close()
	bufioWriter := bufio.NewWriterSize(zstdWriter, 256*1024) // 256KB buffer for efficiency

	if err := d.writeSnapshotHelper(bufioWriter); err != nil {
		return nil, fmt.Errorf("write snapshot: %w", err)
	}

	// Flush and close the writers
	if err := bufioWriter.Flush(); err != nil {
		return nil, fmt.Errorf("flush buffer: %w", err)
	}
	if err := zstdWriter.Close(); err != nil {
		return nil, fmt.Errorf("close zstd writer: %w", err)
	}

	// Close all pipe writers underlying the multiwriter
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			return nil, fmt.Errorf("close pipe writer: %w", err)
		}
	}

	// Wait for all snapshot writes to complete
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("wait for snapshot writes: %w", err)
	}

	return func() error {
		for _, dir := range dirs {
			tmpPath := d.snapshotPath(dir) + ".tmp"
			finalPath := d.snapshotPath(dir)
			if err := os.Rename(tmpPath, finalPath); err != nil {
				return fmt.Errorf("rename snapshot file: %w", err)
			}
		}
		return nil
	}, nil
}

func (d *Disk) writeSnapshotHelper(writer io.Writer) error {
	// create snapshotter
	snapshotter := snapshot.NewSnapshotter(d.FS)

	// create snapshot writer
	snapshotWriter, err := snapshot.NewSnapshotWriter(writer, &gosnapraidpb.SnapshotHeader{
		Version:   1,
		Timestamp: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return fmt.Errorf("create snapshot writer: %w", err)
	}

	// create the snapshot
	if err := snapshotter.Create(snapshotWriter, nil); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	return nil
}
