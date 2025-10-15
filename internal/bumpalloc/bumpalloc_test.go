package bumpalloc

import (
	"bytes"
	"testing"
)

// FuzzAllocator tests the allocator with random inputs to verify:
// 1. All allocations can be retrieved correctly via Get
// 2. Retrieved data matches the originally allocated data
// 3. Handles various buffer sizes (smaller, equal, larger than chunk size)
func FuzzAllocator(f *testing.F) {
	// Add seed corpus with interesting cases
	f.Add([]byte("hello"), 64)
	f.Add([]byte("a"), 16)
	f.Add([]byte("this is a longer string that might exceed chunk size"), 32)
	f.Add([]byte("medium sized buffer for testing"), 128)
	f.Add(make([]byte, 200), 64) // large buffer, small chunk

	f.Fuzz(func(t *testing.T, data []byte, chunkSize int) {
		// Ensure valid chunk size
		if chunkSize < 1 || chunkSize > 10000 {
			t.Skip()
		}

		// Skip empty data as it's not very interesting
		if len(data) == 0 {
			t.Skip()
		}

		alloc := NewAllocator(chunkSize)

		// Perform multiple allocations to test buffer reuse and growth
		segs := make([]Seg, 0, 10)
		originals := make([][]byte, 0, 10)

		// Make several allocations with variations of the input data
		for i := 0; i < 5; i++ {
			// Create variations: full data, prefix, suffix, single byte
			var testData []byte
			switch i % 4 {
			case 0:
				testData = data
			case 1:
				if len(data) > 1 {
					testData = data[:len(data)/2]
				} else {
					testData = data
				}
			case 2:
				if len(data) > 1 {
					testData = data[len(data)/2:]
				} else {
					testData = data
				}
			case 3:
				testData = data[:1]
			}

			// Make a copy to compare against later
			original := make([]byte, len(testData))
			copy(original, testData)

			// Allocate
			seg := alloc.Alloc(testData)
			segs = append(segs, seg)
			originals = append(originals, original)

			// Verify the segment is valid
			if seg.bufIdx < 0 {
				t.Fatalf("allocation %d: invalid bufIdx %d", i, seg.bufIdx)
			}
			if seg.sIdx < 0 || seg.eIdx < seg.sIdx {
				t.Fatalf("allocation %d: invalid indices sIdx=%d eIdx=%d", i, seg.sIdx, seg.eIdx)
			}
			if int(seg.eIdx-seg.sIdx) != len(testData) {
				t.Fatalf("allocation %d: segment size %d doesn't match data size %d",
					i, seg.eIdx-seg.sIdx, len(testData))
			}
		}

		// Now verify all allocations can be retrieved correctly
		for i, seg := range segs {
			retrieved := alloc.Get(seg)
			if retrieved == nil {
				t.Fatalf("allocation %d: Get returned nil for seg %+v", i, seg)
			}
			if !bytes.Equal(retrieved, originals[i]) {
				t.Fatalf("allocation %d: data mismatch\nexpected: %q\ngot:      %q",
					i, originals[i], retrieved)
			}
		}
	})
}

// TestAllocatorBasic provides basic sanity checks
func TestAllocatorBasic(t *testing.T) {
	tests := []struct {
		name      string
		chunkSize int
		allocs    [][]byte
	}{
		{
			name:      "single small allocation",
			chunkSize: 64,
			allocs:    [][]byte{[]byte("hello")},
		},
		{
			name:      "multiple small allocations",
			chunkSize: 64,
			allocs:    [][]byte{[]byte("hello"), []byte("world"), []byte("test")},
		},
		{
			name:      "oversized allocation",
			chunkSize: 16,
			allocs:    [][]byte{[]byte("this is much larger than 16 bytes")},
		},
		{
			name:      "mixed sizes",
			chunkSize: 32,
			allocs: [][]byte{
				[]byte("small"),
				[]byte("this is a medium sized buffer"),
				[]byte("x"),
				[]byte("another medium buffer here"),
				[]byte("this is definitely larger than 32 bytes so it needs its own chunk"),
			},
		},
		{
			name:      "exact chunk size",
			chunkSize: 10,
			allocs:    [][]byte{[]byte("exactly10!")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alloc := NewAllocator(tt.chunkSize)
			segs := make([]Seg, len(tt.allocs))

			// Allocate all buffers
			for i, data := range tt.allocs {
				segs[i] = alloc.Alloc(data)
			}

			// Verify all can be retrieved correctly
			for i, seg := range segs {
				retrieved := alloc.Get(seg)
				if !bytes.Equal(retrieved, tt.allocs[i]) {
					t.Errorf("allocation %d: expected %q, got %q", i, tt.allocs[i], retrieved)
				}
			}
		})
	}
}

// TestAllocatorBufferReuse verifies that buffers are reused efficiently
func TestAllocatorBufferReuse(t *testing.T) {
	alloc := NewAllocator(100)

	// Allocate several small buffers that should fit in one chunk
	seg1 := alloc.Alloc([]byte("first"))  // 5 bytes
	seg2 := alloc.Alloc([]byte("second")) // 6 bytes
	seg3 := alloc.Alloc([]byte("third"))  // 5 bytes

	// They should all be in the same buffer (index 0)
	if seg1.bufIdx != 0 || seg2.bufIdx != 0 || seg3.bufIdx != 0 {
		t.Errorf("expected all allocations in buffer 0, got bufIdx: %d, %d, %d",
			seg1.bufIdx, seg2.bufIdx, seg3.bufIdx)
	}

	// Verify they have sequential positions
	if seg2.sIdx != seg1.eIdx {
		t.Errorf("seg2 should start where seg1 ends: seg1.eIdx=%d, seg2.sIdx=%d",
			seg1.eIdx, seg2.sIdx)
	}
	if seg3.sIdx != seg2.eIdx {
		t.Errorf("seg3 should start where seg2 ends: seg2.eIdx=%d, seg3.sIdx=%d",
			seg2.eIdx, seg3.sIdx)
	}

	// Verify data integrity
	if !bytes.Equal(alloc.Get(seg1), []byte("first")) {
		t.Error("seg1 data corrupted")
	}
	if !bytes.Equal(alloc.Get(seg2), []byte("second")) {
		t.Error("seg2 data corrupted")
	}
	if !bytes.Equal(alloc.Get(seg3), []byte("third")) {
		t.Error("seg3 data corrupted")
	}
}

// BenchmarkAllocator benchmarks the allocator with realistic workload
func BenchmarkAllocator(b *testing.B) {
	sizes := []int{8, 16, 32, 64, 128, 256}

	b.Run("SmallAllocations", func(b *testing.B) {
		alloc := NewAllocator(16 * 1024)
		data := make([]byte, 32)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			alloc.Alloc(data)
		}
	})

	b.Run("MixedSizes", func(b *testing.B) {
		alloc := NewAllocator(16 * 1024)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			size := sizes[i%len(sizes)]
			data := make([]byte, size)
			alloc.Alloc(data)
		}
	})

	b.Run("ManyBuffers", func(b *testing.B) {
		alloc := NewAllocator(16 * 1024)
		data := make([]byte, 100)

		// Pre-allocate many buffers to test fast list performance
		for i := 0; i < 100; i++ {
			for j := 0; j < 8; j++ { // Partially fill each buffer
				alloc.Alloc(data)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			alloc.Alloc(data)
		}
	})

	b.Run("Get", func(b *testing.B) {
		alloc := NewAllocator(4096)
		data := make([]byte, 32)

		// Pre-allocate some segments
		segs := make([]Seg, 1000)
		for i := range segs {
			segs[i] = alloc.Alloc(data)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			seg := segs[i%len(segs)]
			_ = alloc.Get(seg)
		}
	})

	b.Run("AllocAndGet", func(b *testing.B) {
		alloc := NewAllocator(4096)
		data := make([]byte, 32)
		for i := 0; i < len(data); i++ {
			data[i] = byte(i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			seg := alloc.Alloc(data)
			retrieved := alloc.Get(seg)
			if len(retrieved) != len(data) {
				b.Fatal("length mismatch")
			}
		}
	})

	b.Run("MixedAllocAndGet", func(b *testing.B) {
		alloc := NewAllocator(4096)
		segs := make([]Seg, 0, 1000)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%10 < 7 { // 70% allocations
				size := sizes[i%len(sizes)]
				data := make([]byte, size)
				seg := alloc.Alloc(data)
				segs = append(segs, seg)
			} else if len(segs) > 0 { // 30% retrievals
				seg := segs[i%len(segs)]
				_ = alloc.Get(seg)
			}
		}
	})
}
