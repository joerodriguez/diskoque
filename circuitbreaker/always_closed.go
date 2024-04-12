package circuitbreaker

import (
	"github.com/joerodriguez/diskoque"
)

type AlwaysClosed struct{}

func (a AlwaysClosed) State() diskoque.CircuitBreakerState {
	return diskoque.CircuitClosed
}

func (a AlwaysClosed) Success() {}

func (a AlwaysClosed) Failure() {}

func (a AlwaysClosed) ShouldTrial() bool {
	return true
}
