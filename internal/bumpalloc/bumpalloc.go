package bumpalloc

type Seg struct {
	bufIdx int32
	sIdx   int16
	eIdx   int16
}

type Allocator struct {
	chunkSize           int
	buffers             [][]byte
	buffersWithCapacity []int32 // indices of buffers with sufficient remaining capacity
	capacityThreshold   int     // minimum remaining capacity to keep buffer in fast list
}

func NewAllocator(chunkSize int) *Allocator {
	if chunkSize > 65535 {
		panic("chunk size exceeds maximum segment size of 65535 bytes")
	}

	// Set threshold to 1/16 of chunk size, minimum 64 bytes
	threshold := chunkSize / 16
	if threshold < 64 {
		threshold = 64
	}

	return &Allocator{
		chunkSize:           chunkSize,
		buffers:             [][]byte{make([]byte, 0, chunkSize)},
		buffersWithCapacity: []int32{0}, // initial buffer has capacity
		capacityThreshold:   threshold,
	}
}

func (a *Allocator) Alloc(buf []byte) Seg {
	req := len(buf)
	if req > 65535 {
		panic("allocation size exceeds maximum segment size of 65535 bytes")
	}

	// If buf is larger than the standard chunk size, create a dedicated oversized chunk
	if req > a.chunkSize {
		newBuf := make([]byte, 0, req)
		newBuf = append(newBuf, buf...)
		a.buffers = append(a.buffers, newBuf)
		idx := len(a.buffers) - 1
		// Don't add oversized chunks to buffersWithCapacity since they're full
		return Seg{bufIdx: int32(idx), sIdx: 0, eIdx: int16(req)}
	}

	// Loop through buffers with sufficient capacity (fast path)
	for i := 0; i < len(a.buffersWithCapacity); i++ {
		bufIdx := a.buffersWithCapacity[i]
		b := a.buffers[bufIdx]
		remaining := cap(b) - len(b)

		if remaining >= req {
			// Allocate by appending to this buffer
			startIdx := len(b)
			a.buffers[bufIdx] = append(b, buf...)
			endIdx := len(a.buffers[bufIdx])

			// Check if buffer should be removed from fast list
			newRemaining := cap(b) - len(a.buffers[bufIdx])
			if newRemaining < a.capacityThreshold {
				// Remove this buffer from the fast list
				// Swap with last element and truncate
				a.buffersWithCapacity[i] = a.buffersWithCapacity[len(a.buffersWithCapacity)-1]
				a.buffersWithCapacity = a.buffersWithCapacity[:len(a.buffersWithCapacity)-1]
				i-- // Decrement i to recheck the swapped element
			}

			return Seg{bufIdx: int32(bufIdx), sIdx: int16(startIdx), eIdx: int16(endIdx)}
		}
	}

	// No buffer had enough space; create a new standard chunk
	newBuf := make([]byte, 0, a.chunkSize)
	newBuf = append(newBuf, buf...)
	a.buffers = append(a.buffers, newBuf)
	idx := len(a.buffers) - 1

	// Add to buffersWithCapacity if it has enough remaining space
	if a.chunkSize-len(newBuf) >= a.capacityThreshold {
		a.buffersWithCapacity = append(a.buffersWithCapacity, int32(idx))
	}

	return Seg{bufIdx: int32(idx), sIdx: 0, eIdx: int16(len(newBuf))}
}

func (a *Allocator) Get(seg Seg) []byte {
	if int(seg.bufIdx) >= len(a.buffers) {
		return nil
	}
	buf := a.buffers[seg.bufIdx]
	// Get the full underlying array to access all allocated data
	fullBuf := buf[:cap(buf)]
	if int(seg.eIdx) > len(fullBuf) {
		return nil
	}
	return fullBuf[seg.sIdx:seg.eIdx]
}
