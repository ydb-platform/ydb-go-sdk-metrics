package registry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Registry interface {
	// GaugeVec returns GaugeVec by name, subsystem and labels
	// If gauge by args already created - return gauge from cache
	// If gauge by args nothing - create and return newest gauge
	GaugeVec(name string, labelNames ...string) GaugeVec

	// TimerVec returns TimerVec by name, subsystem and labels
	// If timer by args already created - return timer from cache
	// If timer by args nothing - create and return newest timer
	TimerVec(name string, labelNames ...string) TimerVec

	// HistogramVec returns HistogramVec by name, subsystem and labels
	// If histogram by args already created - return histogram from cache
	// If histogram by args nothing - create and return newest histogram
	HistogramVec(name string, labelNames ...string) HistogramVec
}

type Config interface {
	Registry

	// Details returns bitmask for customize details of NewScope
	// If zero - use full set of driver NewScope
	Details() trace.Details

	// WithSystem returns new Config with subsystem scope
	// Separator for split scopes of NewScope provided Config implementation
	WithSystem(subsystem string) Config
}
