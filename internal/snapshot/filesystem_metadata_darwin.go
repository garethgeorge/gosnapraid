//go:build darwin

package snapshot

import (
	"os"
	"syscall"
)

var (
	defaultFileSystem FileSystem = &defaultDarwinFileSystemImpl{}
)

// getUnixMetadata extracts platform-specific metadata from file info
func getUnixMetadata(info os.FileInfo) (metadata unixMetadata, err error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return unixMetadata{}, ErrMetadataNotSupported
	}

	return unixMetadata{
		Ino:        stat.Ino,
		Gen:        uint64(stat.Gen),
		AccessTime: stat.Atimespec.Sec*1000000 + stat.Atimespec.Nsec/1000,
		ChangeTime: stat.Ctimespec.Sec*1000000 + stat.Ctimespec.Nsec/1000,
		BirthTime:  stat.Birthtimespec.Sec*1000000 + stat.Birthtimespec.Nsec/1000,
		UID:        stat.Uid,
		GID:        stat.Gid,
		DeviceID:   uint64(stat.Dev),
	}, nil
}

type defaultDarwinFileSystemImpl struct {
}

func (f *defaultDarwinFileSystemImpl) ReadDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}

func (f *defaultDarwinFileSystemImpl) GetUnixMetadata(info os.FileInfo) (metadata unixMetadata, err error) {
	return getUnixMetadata(info)
}
