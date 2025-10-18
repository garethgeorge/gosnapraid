package sliceutil

import "iter"

func FullOuterJoinSlicesIter[AT, BT any](a []AT, b []BT, less func(a AT, b BT) int) func(yield func(a AT, b BT) bool) {
	var nilAT AT
	var nilBT BT
	i := 0
	j := 0
	return func(yield func(a AT, b BT) bool) {
		for i < len(a) && j < len(b) {
			cmp := less(a[i], b[j])
			if cmp == 0 {
				if !yield(a[i], b[j]) {
					return
				}
				i++
				j++
			} else if cmp < 0 {
				if !yield(a[i], nilBT) {
					return
				}
				i++
			} else {
				if !yield(nilAT, b[j]) {
					return
				}
				j++
			}
		}
		for ; i < len(a); i++ {
			if !yield(a[i], nilBT) {
				return
			}
		}
		for ; j < len(b); j++ {
			if !yield(nilAT, b[j]) {
				return
			}
		}
	}
}

// FullOuterJoinIters takes two sorted iterators and performs a full outer join,
// yielding sorted joined entries based on the less comparator.
// The less function should return:
//   - negative if a < b
//   - zero if a == b
//   - positive if a > b
//
// The function yields pairs of (a, b) where:
//   - Both values are present if they match according to less
//   - Only a is present (b is zero value) if a has no matching b
//   - Only b is present (a is zero value) if b has no matching a
func FullOuterJoinIters[AT, BT any](a iter.Seq[AT], b iter.Seq[BT], less func(a AT, b BT) int) iter.Seq2[AT, BT] {
	var nilAT AT
	var nilBT BT

	return func(yield func(AT, BT) bool) {
		nextA, stopA := iter.Pull(a)
		defer stopA()
		nextB, stopB := iter.Pull(b)
		defer stopB()

		aVal, aOk := nextA()
		bVal, bOk := nextB()

		for aOk && bOk {
			cmp := less(aVal, bVal)
			if cmp == 0 {
				// Both values match
				if !yield(aVal, bVal) {
					return
				}
				aVal, aOk = nextA()
				bVal, bOk = nextB()
			} else if cmp < 0 {
				// a is less than b, yield a with nil b
				if !yield(aVal, nilBT) {
					return
				}
				aVal, aOk = nextA()
			} else {
				// b is less than a, yield nil a with b
				if !yield(nilAT, bVal) {
					return
				}
				bVal, bOk = nextB()
			}
		}

		// Drain remaining elements from a
		for aOk {
			if !yield(aVal, nilBT) {
				return
			}
			aVal, aOk = nextA()
		}

		// Drain remaining elements from b
		for bOk {
			if !yield(nilAT, bVal) {
				return
			}
			bVal, bOk = nextB()
		}
	}
}

// LeftJoinIters takes two sorted iterators and performs a left join,
// yielding sorted joined entries based on the less comparator.
// The less function should return:
//   - negative if a < b
//   - zero if a == b
//   - positive if a > b
//
// The function yields pairs of (a, b) where:
//   - Both values are present if they match according to less
//   - Only a is present (b is zero value) if a has no matching b
//   - Elements from b that don't match any element in a are discarded
//
// All elements from iterator a are guaranteed to be yielded.
func LeftJoinIters[AT, BT any](a iter.Seq[AT], b iter.Seq[BT], less func(a AT, b BT) int) iter.Seq2[AT, BT] {
	var nilBT BT

	return func(yield func(AT, BT) bool) {
		nextA, stopA := iter.Pull(a)
		defer stopA()
		nextB, stopB := iter.Pull(b)
		defer stopB()

		aVal, aOk := nextA()
		bVal, bOk := nextB()

		for aOk {
			if !bOk {
				// No more b values, yield remaining a values with nil b
				if !yield(aVal, nilBT) {
					return
				}
				aVal, aOk = nextA()
				continue
			}

			cmp := less(aVal, bVal)
			if cmp == 0 {
				// Both values match
				if !yield(aVal, bVal) {
					return
				}
				aVal, aOk = nextA()
				bVal, bOk = nextB()
			} else if cmp < 0 {
				// a is less than b, yield a with nil b
				if !yield(aVal, nilBT) {
					return
				}
				aVal, aOk = nextA()
			} else {
				// b is less than a, skip b (not included in left join)
				bVal, bOk = nextB()
			}
		}
	}
}
