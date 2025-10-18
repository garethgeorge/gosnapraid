package sliceutil

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestFullOuterJoinSlices(t *testing.T) {
	// Define the less function for comparing integers
	lessInts := func(a, b int) int {
		return a - b
	}

	// Define a struct for test cases
	testCases := []struct {
		name     string
		a        []int
		b        []int
		less     func(a, b int) int
		expected []struct {
			A int
			B int
		}
	}{
		{
			name: "both slices empty",
			a:    []int{},
			b:    []int{},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{},
		},
		{
			name: "a is empty",
			a:    []int{},
			b:    []int{1, 2, 3},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 0, B: 1},
				{A: 0, B: 2},
				{A: 0, B: 3},
			},
		},
		{
			name: "b is empty",
			a:    []int{1, 2, 3},
			b:    []int{},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 0},
				{A: 2, B: 0},
				{A: 3, B: 0},
			},
		},
		{
			name: "equal slices",
			a:    []int{1, 2, 3},
			b:    []int{1, 2, 3},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 2},
				{A: 3, B: 3},
			},
		},
		{
			name: "a has extra elements at the end",
			a:    []int{1, 2, 3, 4},
			b:    []int{1, 2, 3},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 2},
				{A: 3, B: 3},
				{A: 4, B: 0},
			},
		},
		{
			name: "b has extra elements at the end",
			a:    []int{1, 2, 3},
			b:    []int{1, 2, 3, 4},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 2},
				{A: 3, B: 3},
				{A: 0, B: 4},
			},
		},
		{
			name: "no matches",
			a:    []int{1, 3, 5},
			b:    []int{2, 4, 6},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 0},
				{A: 0, B: 2},
				{A: 3, B: 0},
				{A: 0, B: 4},
				{A: 5, B: 0},
				{A: 0, B: 6},
			},
		},
		{
			name: "mixed matches and mismatches",
			a:    []int{1, 2, 4, 6},
			b:    []int{1, 3, 4, 5},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 0},
				{A: 0, B: 3},
				{A: 4, B: 4},
				{A: 0, B: 5},
				{A: 6, B: 0},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := FullOuterJoinSlices(tc.a, tc.b, tc.less)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("FullOuterJoinSlices() = %v, want %v", result, tc.expected)
			}
		})
	}

	// Test with different types
	t.Run("different types", func(t *testing.T) {
		a := []int{1, 2, 4}
		b := []string{"1", "3", "4"}
		less := func(valA int, valB string) int {
			intB, _ := strconv.Atoi(valB)
			return valA - intB
		}
		expected := []struct {
			A int
			B string
		}{
			{A: 1, B: "1"},
			{A: 2, B: ""},
			{A: 0, B: "3"},
			{A: 4, B: "4"},
		}

		result := FullOuterJoinSlices(a, b, less)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("FullOuterJoinSlices() with different types = %v, want %v", result, expected)
		}
	})

	// Test with strings
	t.Run("strings", func(t *testing.T) {
		a := []string{"a", "c", "e"}
		b := []string{"b", "d", "f"}
		less := func(valA, valB string) int {
			return strings.Compare(valA, valB)
		}
		expected := []struct {
			A string
			B string
		}{
			{A: "a", B: ""},
			{A: "", B: "b"},
			{A: "c", B: ""},
			{A: "", B: "d"},
			{A: "e", B: ""},
			{A: "", B: "f"},
		}

		result := FullOuterJoinSlices(a, b, less)
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("FullOuterJoinSlices() with strings = %v, want %v", result, expected)
		}
	})
}

func TestFullOuterJoinSlicesFunc(t *testing.T) {
	// Define the less function for comparing integers
	lessInts := func(a, b int) int {
		return a - b
	}

	// Define a struct for test cases
	testCases := []struct {
		name     string
		a        []int
		b        []int
		less     func(a, b int) int
		expected []struct {
			A int
			B int
		}
	}{
		{
			name: "both slices empty",
			a:    []int{},
			b:    []int{},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{},
		},
		{
			name: "a is empty",
			a:    []int{},
			b:    []int{1, 2, 3},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 0, B: 1},
				{A: 0, B: 2},
				{A: 0, B: 3},
			},
		},
		{
			name: "b is empty",
			a:    []int{1, 2, 3},
			b:    []int{},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 0},
				{A: 2, B: 0},
				{A: 3, B: 0},
			},
		},
		{
			name: "equal slices",
			a:    []int{1, 2, 3},
			b:    []int{1, 2, 3},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 2},
				{A: 3, B: 3},
			},
		},
		{
			name: "a has extra elements at the end",
			a:    []int{1, 2, 3, 4},
			b:    []int{1, 2, 3},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 2},
				{A: 3, B: 3},
				{A: 4, B: 0},
			},
		},
		{
			name: "b has extra elements at the end",
			a:    []int{1, 2, 3},
			b:    []int{1, 2, 3, 4},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 2},
				{A: 3, B: 3},
				{A: 0, B: 4},
			},
		},
		{
			name: "no matches",
			a:    []int{1, 3, 5},
			b:    []int{2, 4, 6},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 0},
				{A: 0, B: 2},
				{A: 3, B: 0},
				{A: 0, B: 4},
				{A: 5, B: 0},
				{A: 0, B: 6},
			},
		},
		{
			name: "mixed matches and mismatches",
			a:    []int{1, 2, 4, 6},
			b:    []int{1, 3, 4, 5},
			less: lessInts,
			expected: []struct {
				A int
				B int
			}{
				{A: 1, B: 1},
				{A: 2, B: 0},
				{A: 0, B: 3},
				{A: 4, B: 4},
				{A: 0, B: 5},
				{A: 6, B: 0},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result []struct {
				A int
				B int
			} = make([]struct {
				A int
				B int
			}, 0)
			for a, b := range FullOuterJoinSlicesIter(tc.a, tc.b, tc.less) {
				result = append(result, struct {
					A int
					B int
				}{A: a, B: b})
			}
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("FullOuterJoinSlicesFunc() produced %v, want %v", result, tc.expected)
			}
		})
	}

	// Test with different types
	t.Run("different types", func(t *testing.T) {
		a := []int{1, 2, 4}
		b := []string{"1", "3", "4"}
		less := func(valA int, valB string) int {
			intB, _ := strconv.Atoi(valB)
			return valA - intB
		}
		expected := []struct {
			A int
			B string
		}{
			{A: 1, B: "1"},
			{A: 2, B: ""},
			{A: 0, B: "3"},
			{A: 4, B: "4"},
		}

		var result []struct {
			A int
			B string
		}
		for a, b := range FullOuterJoinSlicesIter(a, b, less) {
			result = append(result, struct {
				A int
				B string
			}{A: a, B: b})
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("FullOuterJoinSlicesFunc() with different types = %v, want %v", result, expected)
		}
	})

	// Test with strings
	t.Run("strings", func(t *testing.T) {
		a := []string{"a", "c", "e"}
		b := []string{"b", "d", "f"}
		less := func(valA, valB string) int {
			return strings.Compare(valA, valB)
		}
		expected := []struct {
			A string
			B string
		}{
			{A: "a", B: ""},
			{A: "", B: "b"},
			{A: "c", B: ""},
			{A: "", B: "d"},
			{A: "e", B: ""},
			{A: "", B: "f"},
		}

		var result []struct {
			A string
			B string
		}
		for a, b := range FullOuterJoinSlicesIter(a, b, less) {
			result = append(result, struct {
				A string
				B string
			}{A: a, B: b})
		}
		if !reflect.DeepEqual(result, expected) {
			t.Errorf("FullOuterJoinSlicesFunc() with strings = %v, want %v", result, expected)
		}
	})
}
