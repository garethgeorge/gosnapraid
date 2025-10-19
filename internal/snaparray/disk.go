package snaparray

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/garethgeorge/gosnapraid/internal/progress"
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

// verifySnapshots verifies the snapshots for the disk across all snapshot directories
// where a content manifest could be stored.
// Returns the path of one of the matching snapshots if any exist.
// Returns nil if all snapshots match, or an error if there is a mismatch.
func (d *Disk) verifySnapshots(dirs []string, prog progress.BarProgressTracker) (string, error) {
	var checksumsMu sync.Mutex
	checksums := make(map[string]uint64)

	var eg errgroup.Group
	total := 0
	for _, dir := range dirs {
		path := d.snapshotPath(dir)
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("open snapshot file %s: %w", path, err)
		}
		defer f.Close()
		total++

		eg.Go(func() error {
			// Compute a xxhash64 checksum of the snapshot
			hasher := xxhash.New()
			if _, err := io.Copy(hasher, f); err != nil {
				return fmt.Errorf("hash snapshot file %s: %w", path, err)
			}
			sum := hasher.Sum64()

			checksumsMu.Lock()
			defer checksumsMu.Unlock()
			checksums[path] = sum
			prog.SetDone(len(checksums))
			return nil
		})
	}

	prog.SetMessage("verifying snapshots for disk " + d.ID)
	prog.SetTotal(int64(len(dirs)))
	prog.SetDone(0)

	if err := eg.Wait(); err != nil {
		return "", fmt.Errorf("verify snapshots: %w", err)
	}

	if len(checksums) == 0 {
		return "", nil // No snapshots found
	}

	// Check for mismatched checksums
	distinctChecksums := make(map[uint64][]string)
	for path, sum := range checksums {
		distinctChecksums[sum] = append(distinctChecksums[sum], path)
	}
	if len(distinctChecksums) > 1 {
		var errMap ErrorMap
		errMap.Title = fmt.Sprintf("snapshot checksum mismatch for disk %s", d.ID)
		for checksum, paths := range distinctChecksums {
			errMap.AddError(fmt.Sprintf("checksum %x", checksum), fmt.Errorf("snapshot files: %v", strings.Join(paths, ", ")))
		}
		return "", &errMap
	}

	// All checksums match, return one of the files as a representative.
	for path := range checksums {
		return path, nil
	}
	return "", nil
}

// UpdateSnapshots updates the snapshots for the disk across all snapshot directories
// where the content manifest should be stored.
// Returns a function that can be used to finalize the snapshot update (e.g., renaming temp files).
func (d *Disk) updateSnapshotsHelper(dirs []string, prior string, prog progress.SpinnerProgressTracker) (func() error, error) {
	// If provided, load and use the prior snapshot
	var priorSnapshotReader *snapshot.SnapshotReader
	if prior != "" {
		f, err := os.Open(prior)
		if err != nil {
			return nil, fmt.Errorf("open prior snapshot %s: %w", prior, err)
		}
		defer f.Close()

		zstdReader, err := zstd.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("create zstd reader for prior snapshot %s: %w", prior, err)
		}
		defer zstdReader.Close()

		priorReader, header, err := snapshot.NewSnapshotReader(zstdReader)
		if err != nil {
			return nil, fmt.Errorf("create snapshot reader for prior snapshot %s: %w", prior, err)
		}
		if header.Version != Version {
			return nil, fmt.Errorf("prior snapshot %s has incompatible version %d", prior, header.Version)
		}
		priorSnapshotReader = priorReader
	}

	prog.SetMessage("updating snapshots for disk " + d.ID)

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

	// Create a multiwriter to write to all snapshot pipes with zstd compression.
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

	if err := d.writeSnapshotHelper(bufioWriter, priorSnapshotReader); err != nil {
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

func (d *Disk) writeSnapshotHelper(writer io.Writer, prior *snapshot.SnapshotReader) error {
	// create snapshotter
	snapshotter := snapshot.NewSnapshotter(d.FS)

	// create snapshot writer
	snapshotWriter, err := snapshot.NewSnapshotWriter(writer, &gosnapraidpb.SnapshotHeader{
		Version:   Version,
		Timestamp: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return fmt.Errorf("create snapshot writer: %w", err)
	}

	// create the snapshot
	if err := snapshotter.Create(snapshotWriter, prior); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	return nil
}
