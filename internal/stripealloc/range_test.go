package stripealloc

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRange(t *testing.T) {
	t.Run("Size", func(t *testing.T) {
		testCases := []struct {
			name     string
			r        Range
			expected uint64
		}{
			{"positive size", Range{Start: 10, End: 20}, 10},
			{"zero size", Range{Start: 5, End: 5}, 0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.expected, tc.r.Size())
			})
		}
	})

	t.Run("Less", func(t *testing.T) {
		r1 := Range{Start: 10, End: 20}
		r2 := Range{Start: 20, End: 30}
		r3 := Range{Start: 5, End: 15}

		assert.True(t, r1.Less(r2))
		assert.False(t, r2.Less(r1))
		assert.False(t, r1.Less(r3))
		assert.True(t, r3.Less(r1))
	})

	t.Run("Overlaps", func(t *testing.T) {
		testCases := []struct {
			name     string
			r1, r2   Range
			expected bool
		}{
			{"r2 starts during r1", Range{Start: 10, End: 20}, Range{Start: 15, End: 25}, true},
			{"r1 and r2 are adjacent", Range{Start: 10, End: 20}, Range{Start: 20, End: 30}, false},
			{"r1 starts during r2", Range{Start: 10, End: 20}, Range{Start: 5, End: 15}, true},
			{"r2 contains r1", Range{Start: 10, End: 20}, Range{Start: 5, End: 25}, true},
			{"r1 contains r2", Range{Start: 5, End: 25}, Range{Start: 10, End: 20}, true},
			{"no overlap", Range{Start: 10, End: 20}, Range{Start: 25, End: 30}, false},
			{"identical ranges", Range{Start: 10, End: 20}, Range{Start: 10, End: 20}, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.expected, tc.r1.Overlaps(tc.r2))
				assert.Equal(t, tc.expected, tc.r2.Overlaps(tc.r1))
			})
		}
	})

	t.Run("Adjacent", func(t *testing.T) {
		testCases := []struct {
			name     string
			r1, r2   Range
			expected bool
		}{
			{"r2 starts at r1 end", Range{Start: 10, End: 20}, Range{Start: 20, End: 30}, true},
			{"r1 starts at r2 end", Range{Start: 20, End: 30}, Range{Start: 10, End: 20}, true},
			{"gap between ranges", Range{Start: 10, End: 20}, Range{Start: 21, End: 30}, false},
			{"ranges overlap", Range{Start: 10, End: 20}, Range{Start: 19, End: 29}, false},
			{"identical ranges", Range{Start: 10, End: 20}, Range{Start: 10, End: 20}, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.Equal(t, tc.expected, tc.r1.Adjacent(tc.r2))
				assert.Equal(t, tc.expected, tc.r2.Adjacent(tc.r1))
			})
		}
	})

	t.Run("Merge", func(t *testing.T) {
		testCases := []struct {
			name        string
			r1, r2      Range
			expected    Range
			shouldPanic bool
		}{
			{"overlapping", Range{Start: 10, End: 20}, Range{Start: 15, End: 25}, Range{Start: 10, End: 25}, false},
			{"adjacent", Range{Start: 10, End: 20}, Range{Start: 20, End: 30}, Range{Start: 10, End: 30}, false},
			{"r1 contains r2", Range{Start: 10, End: 30}, Range{Start: 15, End: 25}, Range{Start: 10, End: 30}, false},
			{"r2 contains r1", Range{Start: 15, End: 25}, Range{Start: 10, End: 30}, Range{Start: 10, End: 30}, false},
			{"identical", Range{Start: 10, End: 20}, Range{Start: 10, End: 20}, Range{Start: 10, End: 20}, false},
			{"non-overlapping, non-adjacent", Range{Start: 10, End: 20}, Range{Start: 21, End: 30}, Range{}, true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.shouldPanic {
					assert.Panics(t, func() { tc.r1.Merge(tc.r2) })
					assert.Panics(t, func() { tc.r2.Merge(tc.r1) })
				} else {
					assert.Equal(t, tc.expected, tc.r1.Merge(tc.r2))
					assert.Equal(t, tc.expected, tc.r2.Merge(tc.r1))
				}
			})
		}
	})

	t.Run("String", func(t *testing.T) {
		r := Range{Start: 10, End: 20}
		assert.Equal(t, "[10, 20)", r.String())
	})
}

func TestRangeListFromSlices(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		starts := []uint64{10, 20}
		ends := []uint64{15, 25}
		expected := []Range{{Start: 10, End: 15}, {Start: 20, End: 25}}
		ranges, err := RangeListFromSlices(starts, ends, nil)
		require.NoError(t, err)
		assert.Equal(t, expected, ranges)
	})

	t.Run("mismatched lengths", func(t *testing.T) {
		starts := []uint64{10}
		ends := []uint64{15, 25}
		_, err := RangeListFromSlices(starts, ends, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "starts and ends slices have different lengths")
	})

	t.Run("invalid range", func(t *testing.T) {
		starts := []uint64{20}
		ends := []uint64{10}
		_, err := RangeListFromSlices(starts, ends, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid range: start 20 is greater than end 10")
	})

	t.Run("pre-allocated ranges with enough capacity", func(t *testing.T) {
		starts := []uint64{10, 20}
		ends := []uint64{15, 25}
		prealloc := make([]Range, 5)
		expected := []Range{{Start: 10, End: 15}, {Start: 20, End: 25}}
		ranges, err := RangeListFromSlices(starts, ends, prealloc)
		require.NoError(t, err)
		assert.Equal(t, expected, ranges)
		assert.Equal(t, 5, cap(prealloc)) // check that original slice is used
		assert.Equal(t, 2, len(ranges))
	})

	t.Run("pre-allocated ranges with insufficient capacity", func(t *testing.T) {
		starts := []uint64{10, 20, 30}
		ends := []uint64{15, 25, 35}
		prealloc := make([]Range, 1)
		expected := []Range{{Start: 10, End: 15}, {Start: 20, End: 25}, {Start: 30, End: 35}}
		ranges, err := RangeListFromSlices(starts, ends, prealloc)
		require.NoError(t, err)
		assert.Equal(t, expected, ranges)
		assert.True(t, cap(ranges) >= 3) // check that new slice is allocated
	})
}

func TestRangesToSlices(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		ranges := []Range{{Start: 10, End: 15}, {Start: 20, End: 25}}
		expectedStarts := []uint64{10, 20}
		expectedEnds := []uint64{15, 25}
		starts, ends := RangesToSlices(ranges, nil, nil)
		assert.Equal(t, expectedStarts, starts)
		assert.Equal(t, expectedEnds, ends)
	})

	t.Run("pre-allocated slices with enough capacity", func(t *testing.T) {
		ranges := []Range{{Start: 10, End: 15}, {Start: 20, End: 25}}
		preallocStarts := make([]uint64, 5)
		preallocEnds := make([]uint64, 5)
		expectedStarts := []uint64{10, 20}
		expectedEnds := []uint64{15, 25}
		starts, ends := RangesToSlices(ranges, preallocStarts, preallocEnds)
		assert.Equal(t, expectedStarts, starts)
		assert.Equal(t, expectedEnds, ends)
		assert.Equal(t, 5, cap(preallocStarts))
		assert.Equal(t, 5, cap(preallocEnds))
		assert.Equal(t, 2, len(starts))
		assert.Equal(t, 2, len(ends))
	})

	t.Run("pre-allocated slices with insufficient capacity", func(t *testing.T) {
		ranges := []Range{{Start: 10, End: 15}, {Start: 20, End: 25}, {Start: 30, End: 35}}
		preallocStarts := make([]uint64, 1)
		preallocEnds := make([]uint64, 1)
		expectedStarts := []uint64{10, 20, 30}
		expectedEnds := []uint64{15, 25, 35}
		starts, ends := RangesToSlices(ranges, preallocStarts, preallocEnds)
		assert.Equal(t, expectedStarts, starts)
		assert.Equal(t, expectedEnds, ends)
		assert.True(t, cap(starts) >= 3)
		assert.True(t, cap(ends) >= 3)
	})
}

func FuzzRange_Merge(f *testing.F) {
	f.Add(uint64(10), uint64(20), uint64(15), uint64(25)) // overlapping
	f.Add(uint64(10), uint64(20), uint64(20), uint64(30)) // adjacent
	f.Add(uint64(10), uint64(30), uint64(15), uint64(20)) // containing

	f.Fuzz(func(t *testing.T, s1, e1, s2, e2 uint64) {
		if e1 < s1 || e2 < s2 {
			t.Skip() // Invalid ranges
		}
		r1 := Range{Start: s1, End: e1}
		r2 := Range{Start: s2, End: e2}

		if r1.Overlaps(r2) || r1.Adjacent(r2) {
			merged := r1.Merge(r2)

			// 1. Merged range should contain both original ranges.
			assert.True(t, merged.Start <= r1.Start && merged.End >= r1.End)
			assert.True(t, merged.Start <= r2.Start && merged.End >= r2.End)

			// 2. The size of merged range should be correct.
			maxEnd := r1.End
			if r2.End > maxEnd {
				maxEnd = r2.End
			}
			minStart := r1.Start
			if r2.Start < minStart {
				minStart = r2.Start
			}
			assert.Equal(t, maxEnd-minStart, merged.Size())

		} else {
			// Should panic
			assert.Panics(t, func() { r1.Merge(r2) })
		}
	})
}

func FuzzRange_Slices(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{10, 0, 0, 0, 0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 16 {
			t.Skip()
		}
		numRanges := len(data) / 16
		if numRanges == 0 {
			t.Skip()
		}
		starts := make([]uint64, numRanges)
		ends := make([]uint64, numRanges)

		for i := 0; i < numRanges; i++ {
			s := binary.LittleEndian.Uint64(data[i*16 : i*16+8])
			e := binary.LittleEndian.Uint64(data[i*16+8 : i*16+16])
			if e < s {
				s, e = e, s
			}
			starts[i] = s
			ends[i] = e
		}

		ranges, err := RangeListFromSlices(starts, ends, nil)
		require.NoError(t, err)

		startsOut, endsOut := RangesToSlices(ranges, nil, nil)

		assert.Equal(t, starts, startsOut)
		assert.Equal(t, ends, endsOut)
	})
}