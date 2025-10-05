//go:build linux

package snapshot

import (
	"os"

	"golang.org/x/sys/unix"
)

var (
	defaultFileSystem FileSystem = &defaultLinuxFileSystemImpl{}
)

// getUnixMetadata extracts platform-specific metadata from file info
func getUnixMetadata(info os.FileInfo) (metadata unixMetadata, err error) {
	stat, ok := info.Sys().(*unix.Stat_t)
	if !ok {
		return unixMetadata{}, ErrMetadataNotSupported
	}

	return unixMetadata{
		Ino:        stat.Ino,
		Gen:        uint64(stat.Gen),
		AccessTime: stat.Atim.Sec*1000000 + stat.Atim.Nsec/1000,
		ChangeTime: stat.Ctim.Sec*1000000 + stat.Ctim.Nsec/1000,
		BirthTime:  stat.Btim.Sec*1000000 + stat.Btim.Nsec/1000,
		UID:        uint32(stat.Uid),
		GID:        uint32(stat.Gid),
		DeviceID:   uint64(stat.Dev),
	}, nil
}

type defaultLinuxFileSystemImpl struct {
}

func (f *defaultLinuxFileSystemImpl) ReadDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}

func (f *defaultLinuxFileSystemImpl) GetUnixMetadata(info os.FileInfo) (metadata unixMetadata, err error) {
	return getUnixMetadata(info)
}
