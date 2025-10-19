package poolutil

type Pool[T any] struct {
	New   func() T
	Reset func(T) T
	pool  chan T
}

func NewPool[T any](new func() T, reset func(T) T, size int) *Pool[T] {
	return &Pool[T]{
		New:   new,
		Reset: reset,
		pool:  make(chan T, size),
	}
}

func (p *Pool[T]) Get() T {
	select {
	case item := <-p.pool:
		return item
	default:
		return p.New()
	}
}

func (p *Pool[T]) Put(item T) {
	if p.Reset != nil {
		item = p.Reset(item)
	}
	select {
	case p.pool <- item:
	default:
	}
}
