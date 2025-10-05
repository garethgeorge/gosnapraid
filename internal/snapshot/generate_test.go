package snapshot

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestSort(t *testing.T) {
	var foo = []string{
		"/bar",
		"/bar/baz/fiz",
		"/bar/baz/",
		"/gaz/baz/biz",
		"/",
	}
	slices.SortFunc(foo, func(a, b string) int {
		return strings.Compare(a, b)
	})
	fmt.Println(foo)
	t.Fatalf("DONE")
}
