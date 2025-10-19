package snapraid

import "io/fs"

// Disk represents a single disk in the SnapRAID array.
type Disk struct {
	Path string // Filesystem path to identify the disk
	FS   fs.FS  // Filesystem interface for disk operations
}

type Array struct {
	Data   []*Disk
	Parity []*Disk
}
