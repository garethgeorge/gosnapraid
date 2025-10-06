package blockmap

import (
	"io"

	"github.com/garethgeorge/gosnapraid/internal/binencutil"
)

// EmptyRange is a range that is always empty
var EmptyRange = Range{Start: -1, End: -2}

type Range struct {
	// The start of the range (inclusive)
	Start int64
	// The end of the range (inclusive)
	End int64
}

func (r Range) IsEmpty() bool {
	return r.End < r.Start
}

func (r Range) Serialize(writer io.Writer) error {
	binencutil.WriteUint64(writer, uint64(r.Start))
	binencutil.WriteUint64(writer, uint64(r.End))
	return nil
}

func DeserializeRange(reader io.Reader) (Range, error) {
	start, err := binencutil.ReadUint64(reader)
	if err != nil {
		return Range{}, err
	}
	end, err := binencutil.ReadUint64(reader)
	if err != nil {
		return Range{}, err
	}
	return Range{Start: int64(start), End: int64(end)}, nil
}

func (r Range) Size() int64 {
	if r.End < r.Start {
		return 0
	}
	return r.End - r.Start + 1
}

func (r Range) Overlaps(other Range) bool {
	return r.Start <= other.End && other.Start <= r.End
}

func (r Range) Contains(other Range) bool {
	return r.Start <= other.Start && r.End >= other.End
}
