package snapshot

import (
	"testing"
)

func TestNewSnapshot(t *testing.T) {
	dir := "/tmp/snapshot"
	s := NewSnapshot(dir)
	if s.rootDir != dir {
		t.Errorf("expected rootDir to be %q, got %q", dir, s.rootDir)
	}
}

func FuzzNewSnapshot(f *testing.F) {
	f.Add("/tmp/snapshot")
	f.Fuzz(func(t *testing.T, dir string) {
		s := NewSnapshot(dir)
		if s.rootDir != dir {
			t.Errorf("expected rootDir to be %q, got %q", dir, s.rootDir)
		}
	})
}
