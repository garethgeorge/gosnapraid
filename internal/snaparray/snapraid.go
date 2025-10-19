package snaparray

type Array struct {
	Data         []*Disk
	ParityFiles  []string
	SnapshotDirs []string
}

func NewArray(dataDisks []*Disk, parityFiles, snapshotDirs []string) *Array {
	return &Array{
		Data:         dataDisks,
		ParityFiles:  parityFiles,
		SnapshotDirs: snapshotDirs,
	}
}

func (a *Array) GetDataDisks() []*Disk {
	return a.Data
}

func (a *Array) GetParityFiles() []string {
	return a.ParityFiles
}
