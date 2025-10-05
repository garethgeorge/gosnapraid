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

func FullOuterJoinSlicesFunc[AT, BT any](a []AT, b []BT, less func(a AT, b BT) int, f func(a AT, b BT)) {
	var nilAT AT
	var nilBT BT
	i := 0
	j := 0
	for i < len(a) && j < len(b) {
		cmp := less(a[i], b[j])
		if cmp == 0 {
			f(a[i], b[j])
			i++
			j++
		} else if cmp < 0 {
			f(a[i], nilBT)
			i++
		} else {
			f(nilAT, b[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		f(a[i], nilBT)
	}
	for ; j < len(b); j++ {
		f(nilAT, b[j])
	}
}
