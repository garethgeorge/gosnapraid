package snaparray

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/garethgeorge/gosnapraid/internal/buffers"
	"github.com/garethgeorge/gosnapraid/internal/ioutil"
	"github.com/garethgeorge/gosnapraid/internal/progress"
	"github.com/garethgeorge/gosnapraid/internal/snapshot"
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
func (d *Disk) verifySnapshots(buffers []buffers.CompressedBufferHandle, prog progress.BarProgressTracker) (string, error) {
	var checksumsMu sync.Mutex
	checksums := make(map[string]uint64)

	var eg errgroup.Group
	total := 0
	for _, buffer := range buffers {
		reader, err := buffer.GetRaw().GetReader() // Get the raw reader to avoid double compression
		if err != nil {
			return "", fmt.Errorf("get snapshot reader %q: %w", buffer.Name(), err)
		}
		defer reader.Close()

		f := reader
		total++

		eg.Go(func() error {
			// Compute a xxhash64 checksum of the snapshot
			hasher := xxhash.New()
			if _, err := io.Copy(hasher, f); err != nil {
				return fmt.Errorf("hash snapshot file %s: %w", buffer.Name(), err)
			}
			sum := hasher.Sum64()

			checksumsMu.Lock()
			defer checksumsMu.Unlock()
			checksums[buffer.Name()] = sum
			prog.SetDone(len(checksums))
			return nil
		})
	}

	prog.SetMessage("verifying snapshots for disk " + d.ID)
	prog.SetTotal(int64(len(buffers)))
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
		closers = append(closers, writer)
		defer writer.Close()
		if err != nil {
			return snapshot.SnapshotStats{}, fmt.Errorf("get snapshot writer %q: %w", buf.Name(), err)
		}
		writers = append(writers, writer)
	}
	// Create a multiwriter that writes to all writers
	// and closes the underlying writers when closed.
	multiwriter := ioutil.ParallelMultiWriter(writers...)

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
	if err := ioutil.NewMultiCloser(closers...).Close(); err != nil {
		return stats, fmt.Errorf("close snapshot writers: %w", err)
	}

	return stats, nil
}

func (d *Disk) writeSnapshotHelper(newSnapshot buffers.BufferHandle, prior buffers.BufferHandle) (snapshot.SnapshotStats, error) {
	// create snapshotter
	snapshotter := snapshot.NewSnapshotter(d.FS, newSnapshot, prior)

	return stats, nil
}
