package progress

type SpinnerProgressTracker interface {
	SetMessage(msg string)
	SetDone(n int)
	SetError(err error)
	MarkFinished()
}

type NoopSpinnerProgressTracker struct{}

func (n NoopSpinnerProgressTracker) SetMessage(msg string) {}
func (n NoopSpinnerProgressTracker) AddDone(n2 int)        {}
func (n NoopSpinnerProgressTracker) AddError(err error)    {}

type BarProgressTracker interface {
	SetMessage(msg string)
	SetTotal(total int64)
	SetDone(n int)
	SetError(err error)
	MarkFinished()
}

type NoopBarProgressTracker struct{}

func (n NoopBarProgressTracker) SetTotal(total int64) {}
func (n NoopBarProgressTracker) AddDone(n2 int)       {}
func (n NoopBarProgressTracker) AddError(err error)   {}
