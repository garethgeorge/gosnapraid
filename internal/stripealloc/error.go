package stripealloc

var (
	ErrNoCapacity       = &AllocError{"no capacity available for allocation"}
	ErrAlreadyAllocated = &AllocError{"requested range is already allocated"}
)

type AllocError struct {
	Msg string
}

func (e *AllocError) Error() string {
	return e.Msg
}

func (e *AllocError) Is(target error) bool {
	if targetErr, ok := target.(*AllocError); ok {
		return e.Msg == targetErr.Msg
	}
	return false
}
