package stripealloc

import "fmt"

var EmptyRange = Range{Start: 0, End: 0}

type Range struct {
	Start uint64 // inclusive
	End   uint64 // exclusive
}

func (r Range) Size() uint64 {
	return r.End - r.Start
}

func (r Range) Less(other Range) bool {
	return r.Start < other.Start
}

func (r Range) Overlaps(other Range) bool {
	return r.Start < other.End && other.Start < r.End
}

func (r Range) Adjacent(other Range) bool {
	return r.End == other.Start || other.End == r.Start
}

func (r Range) Merge(other Range) Range {
	if !r.Overlaps(other) && !r.Adjacent(other) {
		panic("cannot merge non-overlapping, non-adjacent ranges")
	}
	start := r.Start
	if other.Start < start {
		start = other.Start
	}
	end := r.End
	if other.End > end {
		end = other.End
	}
	return Range{Start: start, End: end}
}

func (r Range) String() string {
	return fmt.Sprintf("[%d, %d)", r.Start, r.End)
}

func RangeListFromSlices(starts []uint64, ends []uint64, ranges []Range) ([]Range, error) {
	if len(starts) != len(ends) {
		return nil, fmt.Errorf("starts and ends slices have different lengths: %d vs %d", len(starts), len(ends))
	}
	if ranges == nil || cap(ranges) < len(starts) {
		ranges = make([]Range, len(starts))
	} else {
		ranges = ranges[:len(starts)]
	}
	for i := range starts {
		if ends[i] < starts[i] {
			return nil, fmt.Errorf("invalid range: start %d is greater than end %d", starts[i], ends[i])
		}
		ranges[i] = Range{Start: starts[i], End: ends[i]}
	}
	return ranges, nil
}

func RangesToSlices(ranges []Range, starts []uint64, ends []uint64) (startsOut []uint64, endsOut []uint64) {
	if cap(starts) < len(ranges) {
		starts = make([]uint64, len(ranges))
	} else {
		starts = starts[:len(ranges)]
	}
	if cap(ends) < len(ranges) {
		ends = make([]uint64, len(ranges))
	} else {
		ends = ends[:len(ranges)]
	}
	for i, r := range ranges {
		starts[i] = r.Start
		ends[i] = r.End
	}
	return starts, ends
}
