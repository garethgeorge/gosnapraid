package snapshot

// Snapshot represents a snapshot of a directory tree at a point in time.
type Snapshot struct {
	// Path to which the snapshot will be written (on disk)
	rootDir string
}

func NewSnapshot(dir string) *Snapshot {
	return &Snapshot{rootDir: dir}
}
