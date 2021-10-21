package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Label struct {
	Tag   string
	Value string
}

const (
	TagVersion    = "sdk"
	TagSource     = "source"
	TagName       = "name"
	TagMethod     = "method"
	TagError      = "error"
	TagErrCode    = "errCode"
	TagAddress    = "address"
	TagNodeID     = "nodeID"
	TagDataCenter = "destination"
	TagState      = "state"
	TagIdempotent = "idempotent"
	TagSuccess    = "success"
	TagStage      = "stage"
)

type Name string

func err(err error, labels ...Label) []Label {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "network/" + netErr.Op + " -> " + netErr.Err.Error(),
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, io.EOF) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "io/EOF",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "context/DeadlineExceeded",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.Canceled) {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "context/Canceled",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if ok, code, text := ydb.IsTransportError(err); ok {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "transport/" + text,
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", code),
			},
		)
	}
	if ok, code, text := ydb.IsOperationError(err); ok {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "operation/" + text,
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", code),
			},
		)
	}
	return append(
		labels,
		Label{
			Tag:   TagError,
			Value: "unknown/" + strings.ReplaceAll(err.Error(), " ", "_"),
		},
		Label{
			Tag:   TagErrCode,
			Value: "-1",
		},
	)
}

// Gauge tracks single float64 value.
type Gauge interface {
	Add(delta float64)
	Set(value float64)
}

// GaugeVec returns Gauge from GaugeVec by lables
type GaugeVec interface {
	With(labels ...Label) Gauge
}

type Details int

const (
	DriverSystemEvents = Details(1 << iota)
	DriverClusterEvents
	driverNetEvents
	DriverCoreEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents

	tableSessionEvents
	tableSessionQueryInvokeEvents
	tableSessionQueryStreamEvents
	tableSessionTransactionEvents
	tablePoolLifeCycleEvents
	tablePoolRetryEvents
	tablePoolSessionLifeCycleEvents
	tablePoolCommonAPIEvents
	tablePoolNativeAPIEvents
	tablePoolYdbSqlAPIEvents

	DriverConnEvents        = driverNetEvents | DriverCoreEvents
	tableSessionQueryEvents = tableSessionQueryInvokeEvents | tableSessionQueryStreamEvents
	TableSessionEvents      = tableSessionEvents | tableSessionQueryEvents | tableSessionTransactionEvents
	TablePoolEvents         = tablePoolLifeCycleEvents | tablePoolRetryEvents | tablePoolSessionLifeCycleEvents | tablePoolCommonAPIEvents | tablePoolNativeAPIEvents | tablePoolYdbSqlAPIEvents
)

var (
	version = Label{
		Tag: TagVersion,
		Value: func() string {
			_, version := path.Split(ydb.Version)
			return version
		}(),
	}
)

type Config interface {
	// Details returns bitmask for customize details of metrics
	// If zero - use full set of driver metrics
	Details() Details

	// GaugeVec returns GaugeVec by name, subsystem and labels
	// If gauge by args already created - return gauge from cache
	// If gauge by args nothing - create and return newest gauge
	GaugeVec(name string, description string, labelNames ...string) GaugeVec

	// WithSystem returns new Config with subsystem scope
	// Separator for split scopes of metrics provided Config implementation
	WithSystem(subsystem string) Config
}

func ifStr(cond bool, true, false string) string {
	if cond {
		return true
	}
	return false
}

type callScope struct {
	latency GaugeVec
	calls   GaugeVec
	value   GaugeVec
	errs    GaugeVec
}

func (s callScope) start(labels ...Label) *callTrace {
	s.calls.With(
		version,
		Label{
			Tag:   TagSuccess,
			Value: "wip",
		},
	).Add(1)
	return &callTrace{
		start: time.Now(),
		scope: &s,
	}
}

type callTrace struct {
	start time.Time
	scope *callScope
}

func (t *callTrace) syncWithValue(error error, value float64, labels ...Label) {
	t.sync(error, labels...)
	t.scope.value.With(append([]Label{version}, labels...)...).Set(value)
}

func (t *callTrace) syncWithSuccess(ok bool, labels ...Label) {
	success := Label{
		Tag:   TagSuccess,
		Value: ifStr(ok, "true", "false"),
	}
	t.scope.calls.With(append([]Label{version, success}, labels...)...).Add(1)
	t.scope.latency.With(append([]Label{version, success}, labels...)...).Set(float64(time.Since(t.start).Nanoseconds()))
}

func (t *callTrace) sync(error error, labels ...Label) {
	success := Label{
		Tag:   TagSuccess,
		Value: ifStr(err == nil, "true", "false"),
	}
	t.scope.calls.With(append([]Label{version, success}, labels...)...).Add(1)
	t.scope.latency.With(append([]Label{version, success}, labels...)...).Set(float64(time.Since(t.start).Nanoseconds()))
	if err != nil {
		t.scope.errs.With(err(error, append([]Label{version, success}, labels...)...)...).Add(1)
	}
}

func callGauges(c Config, scope string, tags ...string) (s *callScope) {
	c = c.WithSystem(scope)
	return &callScope{
		latency: c.GaugeVec("latency", "latency of "+scope, append([]string{TagSuccess, TagVersion}, tags...)...),
		calls:   c.GaugeVec("calls", "calls of "+scope, append([]string{TagSuccess, TagVersion}, tags...)...),
		value:   c.GaugeVec("value", "value of "+scope, append([]string{TagVersion}, tags...)...),
		errs:    c.GaugeVec("errors", "errors of "+scope, append([]string{TagVersion, TagError, TagErrCode}, tags...)...),
	}
}
