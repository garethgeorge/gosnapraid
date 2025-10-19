package snaparray

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/garethgeorge/gosnapraid/internal/blockmap"
	"github.com/garethgeorge/gosnapraid/internal/buffers"
	"github.com/garethgeorge/gosnapraid/internal/ioutil"
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

func (d *Disk) updateSnapshots(outputsDirs []string, prior string, prog progress.SpinnerProgressTracker) (snapshot.SnapshotStats, error) {
	// Create the buffer handles for each potential output
	var bufs []buffers.CompressedBufferHandle
	for _, dir := range outputsDirs {
		path := d.snapshotPath(dir) + ".tmp"
		bufHandle := buffers.CreateCompressedHandle(buffers.BufferHandleFromFile(path))
		bufs = append(bufs, bufHandle)
	}

	// Open prior snapshot if provided
	var priorBuf buffers.CompressedBufferHandle
	if prior != "" {
		priorBuf = buffers.CreateCompressedHandle(buffers.BufferHandleFromFile(prior))
	}

	stats, err := d.updateSnapshotsHelper(bufs, priorBuf, prog)
	if err != nil {
		return stats, fmt.Errorf("update snapshots: %w", err)
	}

	// Rename temp files to final snapshot files
	for _, dir := range outputsDirs {
		tmpPath := d.snapshotPath(dir) + ".tmp"
		finalPath := d.snapshotPath(dir)
		if err := os.Rename(tmpPath, finalPath); err != nil {
			return stats, fmt.Errorf("rename snapshot file %s to %s: %w", tmpPath, finalPath, err)
		}
	}

	return stats, nil
}

// UpdateSnapshots updates the snapshots for the disk across all snapshot directories
// where the content manifest should be stored.
// Returns a function that can be used to finalize the snapshot update (e.g., renaming temp files).
func (d *Disk) updateSnapshotsHelper(outbuffers []buffers.CompressedBufferHandle, prior buffers.CompressedBufferHandle, prog progress.SpinnerProgressTracker) (snapshot.SnapshotStats, error) {
	prog.SetMessage("updating snapshots for disk " + d.ID)

	// Create the writer end which will write to all output buffers in parallel
	var writers []io.Writer
	var closers []io.Closer
	for _, buf := range outbuffers {
		writer, err := buf.GetRaw().GetWriter() // Get the raw writer to avoid double compression
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("get snapshot writer %q: %w", buf.Name(), err)
		}
		writers = append(writers, writer)
		closers = append(closers, writer)
	}
	multiwriter := ioutil.WithWriterCloser(ioutil.ParallelMultiWriter(writers...), func() error {
		var err error
		for _, closer := range closers {
			if e := closer.Close(); e != nil {
				err = e
			}
		}
		return err
	})

	zstdWriter, err := zstd.NewWriter(multiwriter,
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(4),
		zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return snapshot.SnapshotStats{}, fmt.Errorf("create zstd writer: %w", err)
	}
	defer zstdWriter.Close()

	stats, err := d.writeSnapshotHelper(zstdWriter, prior)
	if err != nil {
		return stats, fmt.Errorf("write snapshot: %w", err)
	}

	// Flush and close the zstd writer
	if err := zstdWriter.Close(); err != nil {
		return stats, fmt.Errorf("close zstd writer: %w", err)
	}

	// Close all pipe writers underlying the multiwriter
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			return stats, fmt.Errorf("close pipe writer: %w", err)
		}
	}

	// Wait for all snapshot writes to complete
	if err := eg.Wait(); err != nil {
		return stats, fmt.Errorf("wait for snapshot writes: %w", err)
	}

	return stats, nil
}

func (d *Disk) writeSnapshotHelper(writer io.Writer, prior buffers.BufferHandle) (snapshot.SnapshotStats, error) {
	// create snapshotter
	snapshotter := snapshot.NewSnapshotter(d.FS)

	// create snapshot writer
	snapshotWriter, err := snapshot.NewSnapshotWriter(writer, &gosnapraidpb.SnapshotHeader{
		Version:   Version,
		Timestamp: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return snapshot.SnapshotStats{}, fmt.Errorf("create snapshot writer: %w", err)
	}

	// reopen prior snapshot and populate dedupe tree
	if prior != nil {
		priorReadCloser, err := prior.GetReader()
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("get prior snapshot reader: %w", err)
		}
		defer priorReadCloser.Close()
		reader, header, err := snapshot.NewSnapshotReader(priorReadCloser)
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("open prior snapshot: %w", err)
		}
		if header.Version != Version {
			return snapshot.SnapshotStats{}, fmt.Errorf("prior snapshot version %d does not match current version %d", header.Version, Version)
		}
		err = snapshotter.UseMoveDetection(reader)
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("enable move detection: %w", err)
		}
		priorReadCloser.Close()
	}

	// open prior snapshot for the diffing scan
	var priorSnapshot *snapshot.SnapshotReader
	if prior != nil {
		priorReadCloser, err := prior.GetReader()
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("get prior snapshot reader: %w", err)
		}
		defer priorReadCloser.Close()
		reader, _, err := snapshot.NewSnapshotReader(priorReadCloser)
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("open prior snapshot: %w", err)
		}
		priorSnapshot = reader
	}

	// create the snapshot
	stats, err := snapshotter.Create(snapshotWriter, priorSnapshot)
	if err != nil {
		return snapshot.SnapshotStats{}, fmt.Errorf("create snapshot: %w", err)
	}

	return stats, nil
}

func (d *Disk) loadAllocationMap(snapshotBuf buffers.CompressedBufferHandle) (*blockmap.RangeAllocator[struct{}], error) {
	reader, err := snapshotBuf.GetReader()
	if err != nil {
		return nil, fmt.Errorf("get snapshot reader: %w", err)
	}
	defer reader.Close()

	snapshotReader, header, err := snapshot.NewSnapshotReader(reader)
	if err != nil {
		return nil, fmt.Errorf("open snapshot reader: %w", err)
	}
	if header.Version != Version {
		return nil, fmt.Errorf("snapshot version %d does not match expected version %d", header.Version, Version)
	}

	rangealloc := blockmap.NewRangeAllocator[struct{}](0, 1<<63-1) // Use max int64 as upper bound
	for entry, err := range snapshotReader.Iter() {
		if err != nil {
			return nil, fmt.Errorf("iterate snapshot entries: %w", err)
		}

		for i := 0; i < len(entry.SliceRangeStarts) && i < len(entry.SliceRangeEnds); i++ {
			start := entry.SliceRangeStarts[i]
			end := entry.SliceRangeEnds[i]
			if !rangealloc.SetAllocated(int64(start), int64(end), struct{}{}) {
				return nil, fmt.Errorf("block range overlap detected for file %s at offset %d length %d", entry.Path, start, end)
			}
		}
	}

	return rangealloc, nil
}
