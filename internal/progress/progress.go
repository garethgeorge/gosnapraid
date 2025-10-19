package progress

type SpinnerProgressTracker interface {
	SetMessage(msg string)
	SetDone(n int)
	SetError(err error)
	MarkFinished()
}

type NoopSpinnerProgressTracker struct{}

var _ SpinnerProgressTracker = NoopSpinnerProgressTracker{}

func (n NoopSpinnerProgressTracker) SetMessage(msg string) {}
func (n NoopSpinnerProgressTracker) SetDone(n2 int)        {}
func (n NoopSpinnerProgressTracker) SetError(err error)    {}
func (n NoopSpinnerProgressTracker) MarkFinished()         {}

type BarProgressTracker interface {
	SetMessage(msg string)
	SetTotal(total int64)
	SetDone(n int)
	SetError(err error)
	MarkFinished()
}

type NoopBarProgressTracker struct{}

var _ BarProgressTracker = NoopBarProgressTracker{}

func (n NoopBarProgressTracker) SetMessage(msg string) {}
func (n NoopBarProgressTracker) SetTotal(total int64)  {}
func (n NoopBarProgressTracker) SetDone(n2 int)        {}
func (n NoopBarProgressTracker) SetError(err error)    {}
func (n NoopBarProgressTracker) MarkFinished()         {}
