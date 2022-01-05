package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
	if d := ydb.TransportErrorDescription(err); d != nil {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "transport/" + d.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", d.Code()),
			},
		)
	}
	if d := ydb.OperationErrorDescription(err); d != nil {
		return append(
			labels,
			Label{
				Tag:   TagError,
				Value: "operation/" + d.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", d.Code()),
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
	With(map[string]string) Gauge
}

// TimerVec stores multiple dynamically created timers
type TimerVec interface {
	With(map[string]string) Timer
}

// Timer tracks distribution of value.
type Timer interface {
	Record(d time.Duration)
}

type Registry interface {
	// GaugeVec returns GaugeVec by name, subsystem and labels
	// If gauge by args already created - return gauge from cache
	// If gauge by args nothing - create and return newest gauge
	GaugeVec(name string, labelNames ...string) GaugeVec

	// TimerVec returns TimerVec by name, subsystem and labels
	// If timer by args already created - return timer from cache
	// If timer by args nothing - create and return newest timer
	TimerVec(name string, labelNames ...string) TimerVec
}

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
	Registry

	// Details returns bitmask for customize details of metrics
	// If zero - use full set of driver metrics
	Details() trace.Details

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
	latency TimerVec
	calls   GaugeVec
	value   GaugeVec
	errs    GaugeVec
}

func labelsToKeyValue(labels ...Label) map[string]string {
	kv := make(map[string]string, len(labels))
	for _, l := range labels {
		kv[l.Tag] = l.Value
	}
	return kv
}

func (s callScope) start(labels ...Label) *callTrace {
	s.calls.With(labelsToKeyValue(
		append([]Label{version, {
			Tag:   TagSuccess,
			Value: "wip",
		}}, labels...)...,
	)).Add(1)
	return &callTrace{
		scope: &s,
		start: time.Now(),
	}
}

type callTrace struct {
	scope *callScope
	start time.Time
}

func (t *callTrace) syncWithValue(error error, value float64, labels ...Label) {
	t.syncWithSuccess(error == nil, labels...)
	if error == nil {
		t.scope.value.With(labelsToKeyValue(append([]Label{version}, labels...)...)).Set(value)
	}
}

func (t *callTrace) syncWithSuccess(ok bool, labels ...Label) (callLabels []Label) {
	success := Label{
		Tag:   TagSuccess,
		Value: ifStr(ok, "true", "false"),
	}
	t.scope.calls.With(labelsToKeyValue(append([]Label{version, success}, labels...)...)).Add(1)
	t.scope.latency.With(labelsToKeyValue(append([]Label{version, success}, labels...)...)).Record(time.Since(t.start))
	return append([]Label{version, success}, labels...)
}

func (t *callTrace) sync(e error, labels ...Label) (callLabels []Label, errLabels []Label) {
	callLabels = t.syncWithSuccess(e == nil, labels...)
	if e != nil {
		errLabels = err(e, append([]Label{version}, labels...)...)
		t.scope.errs.With(labelsToKeyValue(errLabels...)).Add(1)
	}
	return
}

func metrics(c Config, scope string, tags ...string) (s *callScope) {
	c = c.WithSystem(scope)
	return &callScope{
		latency: c.TimerVec("latency", append([]string{TagSuccess, TagVersion}, tags...)...),
		calls:   c.GaugeVec("calls", append([]string{TagSuccess, TagVersion}, tags...)...),
		value:   c.GaugeVec("value", append([]string{TagVersion}, tags...)...),
		errs:    c.GaugeVec("errors", append([]string{TagVersion, TagError, TagErrCode}, tags...)...),
	}
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		panic(err)
	}
	return u.Query().Get("node_id")
}
