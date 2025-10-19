package snaparray

type SpinnerProgressTracker interface {
	SetMessage(msg string)
	AddDone(n int)
	AddError(err error)
}
type NoopSpinnerProgressTracker struct{}

func (n NoopSpinnerProgressTracker) SetMessage(msg string) {}
func (n NoopSpinnerProgressTracker) AddDone(n2 int)        {}
func (n NoopSpinnerProgressTracker) AddError(err error)    {}

type BarProgressTracker interface {
	SetTotal(total int64)
	AddDone(n int)
	AddError(err error)
}

type NoopBarProgressTracker struct{}

func (n NoopBarProgressTracker) SetTotal(total int64) {}
func (n NoopBarProgressTracker) AddDone(n2 int)       {}
func (n NoopBarProgressTracker) AddError(err error)   {}
