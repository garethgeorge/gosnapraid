package bigsort

// valueAndSource holds a value of type T along with its source channel index.
type valueAndSource[T BigSortable] struct {
	batch      []T
	idx        int
	sourceChan chan []T // channel from which to refill the batch
	reuseChan  chan []T // channel to return used batches
}

func (val *valueAndSource[T]) Advance() bool {
	val.idx++
	if val.idx >= len(val.batch) {
		// Return the used batch for reuse
		if val.reuseChan != nil && val.batch != nil {
			select {
			case val.reuseChan <- val.batch:
			default:
			}
		}

		// Try to read the next item from the source channel
		nextItem, ok := <-val.sourceChan
		if !ok {
			// No more items in this source
			return false
		}
		val.batch = nextItem
		val.idx = 0
	}
	return true
}

// Current returns the current value from the batch.
func (vas *valueAndSource[T]) Current() T {
	return vas.batch[vas.idx]
}

// Less compares the Current value of two valueAndSource pointers.
func (vas *valueAndSource[T]) Less(other *valueAndSource[T]) bool {
	return vas.Current().Less(other.Current())
}

// valueHeap is a min-heap of *valueAndSource pointers.
type valueHeap[T BigSortable] []*valueAndSource[T]

func (h valueHeap[T]) Len() int {
	return len(h)
}

func (h valueHeap[T]) Less(i, j int) bool {
	// We want a min-heap, so we use the Less method directly.
	return h[i].Less(h[j])
}

func (h valueHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *valueHeap[T]) Push(x any) {
	*h = append(*h, x.(*valueAndSource[T]))
}

func (h *valueHeap[T]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return item
}
