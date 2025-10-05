package sliceutil

func FullOuterJoinSlices[AT, BT any](a []AT, b []BT, less func(a AT, b BT) int) []struct {
	A AT
	B BT
} {
	var nilAT AT
	var nilBT BT
	result := make([]struct {
		A AT
		B BT
	}, 0, len(a)+len(b))
	i := 0
	j := 0
	for i < len(a) && j < len(b) {
		cmp := less(a[i], b[j])
		if cmp == 0 {
			result = append(result, struct {
				A AT
				B BT
			}{
				A: a[i],
				B: b[j],
			})
			i++
			j++
		} else if cmp < 0 {
			result = append(result, struct {
				A AT
				B BT
			}{
				A: a[i],
				B: nilBT,
			})
			i++
		} else {
			result = append(result, struct {
				A AT
				B BT
			}{
				A: nilAT,
				B: b[j],
			})
			j++
		}
	}
	for ; i < len(a); i++ {
		result = append(result, struct {
			A AT
			B BT
		}{
			A: a[i],
			B: nilBT,
		})
	}
	for ; j < len(b); j++ {
		result = append(result, struct {
			A AT
			B BT
		}{
			A: nilAT,
			B: b[j],
		})
	}
	return result
}

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
