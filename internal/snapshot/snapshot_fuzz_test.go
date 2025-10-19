package snapshot

import (
	"bytes"
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/snapshot/testutil"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
)

func snapshotForConfig(config testutil.TreeConfig) []byte {
	mapFS := testutil.GenerateMapFS(config)

	var buf bytes.Buffer
	snapshotter := NewSnapshotter(mapFS)
	writer, err := NewSnapshotWriter(&buf, &gosnapraidpb.SnapshotHeader{Version: 1})
	if err != nil {
		panic("failed to create snapshot writer: " + err.Error())
	}

	err = snapshotter.Create(writer, nil)
	if err != nil {
		panic("failed to create snapshot: " + err.Error())
	}

	bytes := buf.Bytes()
	return bytes
}

func FuzzSnapshotterCreate(f *testing.F) {
	// Seed 1: Valid tree config and a matching prior snapshot
	config := testutil.DefaultTreeConfig()

	f.Add(config.BreadthPerDir, config.FilesPerDir, config.FileSize, snapshotForConfig(config))
	f.Add(config.BreadthPerDir, config.FilesPerDir, config.FileSize, []byte{}) // No prior snapshot

	f.Fuzz(func(t *testing.T, breadthPerDir, filesPerDir, fileSize int, priorSnapshotData []byte) {
		config := testutil.DefaultTreeConfig()
		config.BreadthPerDir = breadthPerDir
		config.FilesPerDir = filesPerDir
		config.FileSize = fileSize

		// Prevent overly large filesystems from being generated
		config.Depth = config.Depth % 4
		config.BreadthPerDir = config.BreadthPerDir % 5
		config.FilesPerDir = config.FilesPerDir % 10

		mapFS := testutil.GenerateMapFS(config)

		priorReader, _, _ := NewSnapshotReader(bytes.NewReader(priorSnapshotData))

		var newBuf bytes.Buffer
		newWriter, err := NewSnapshotWriter(&newBuf, &gosnapraidpb.SnapshotHeader{Version: 1})
		if err != nil {
			t.Fatal("failed to create snapshot writer:", err)
		}

		snapshotter := NewSnapshotter(mapFS)
		if err := snapshotter.Create(newWriter, priorReader); err != nil {
			t.Fatalf("snapshot creation failed: %v", err)
		}
	})
}
