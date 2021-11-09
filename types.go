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
	"sync"
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
	With(map[string]string) Gauge
}

type Registry interface {
	GaugeVec(name string, labels []string) GaugeVec
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
	// Details returns bitmask for customize details of metrics
	// If zero - use full set of driver metrics
	Details() trace.Details

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

func labelsToKeyValue(labels ...Label) map[string]string {
	kv := make(map[string]string, len(labels))
	for _, l := range labels {
		kv[l.Tag] = l.Value
	}
	return kv
}

func (s callScope) start(labels ...Label) *callTrace {
	s.calls.With(labelsToKeyValue(
		append([]Label{version, Label{
			Tag:   TagSuccess,
			Value: "wip",
		}}, labels...)...,
	)).Add(1)
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
	if error == nil {
		t.scope.value.With(labelsToKeyValue(append([]Label{version}, labels...)...)).Set(value)
	}
}

func (t *callTrace) syncWithSuccess(ok bool, labels ...Label) {
	success := Label{
		Tag:   TagSuccess,
		Value: ifStr(ok, "true", "false"),
	}
	t.scope.calls.With(labelsToKeyValue(append([]Label{version, success}, labels...)...)).Add(1)
	t.scope.latency.With(labelsToKeyValue(append([]Label{version, success}, labels...)...)).Set(float64(time.Since(t.start).Nanoseconds()))
}

func (t *callTrace) sync(e error, labels ...Label) (callLables []Label, errLabels []Label) {
	success := Label{
		Tag:   TagSuccess,
		Value: ifStr(e == nil, "true", "false"),
	}
	callLables = append([]Label{version, success}, labels...)
	t.scope.calls.With(labelsToKeyValue(callLables...)).Add(1)
	t.scope.latency.With(labelsToKeyValue(callLables...)).Set(float64(time.Since(t.start).Nanoseconds()))
	if e != nil {
		errLabels = err(e, append([]Label{version}, labels...)...)
		t.scope.errs.With(labelsToKeyValue(errLabels...)).Add(1)
	}
	return
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

type config struct {
	details   trace.Details
	separator string
	registry  Registry
	namespace string

	m      sync.Mutex
	gauges map[string]GaugeVec
}

func (c *config) join(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return ""
	}
	return strings.Join([]string{a, b}, c.separator)
}

func (c *config) WithSystem(subsystem string) Config {
	return &config{
		separator: c.separator,
		details:   c.details,
		registry:  c.registry,
		namespace: c.join(c.namespace, subsystem),
		gauges:    make(map[string]GaugeVec),
	}
}

func (c *config) Details() trace.Details {
	return c.details
}

func (c *config) GaugeVec(name string, _ string, labelNames ...string) GaugeVec {
	name = c.join(c.namespace, name)
	c.m.Lock()
	defer c.m.Unlock()
	if g, ok := c.gauges[name]; ok {
		return g
	}
	g := c.registry.GaugeVec(name, append([]string{}, labelNames...))
	c.gauges[name] = g
	return g
}

type option func(*config)

func WithNamespace(namespace string) option {
	return func(c *config) {
		c.namespace = namespace
	}
}

func WithDetails(details trace.Details) option {
	return func(c *config) {
		c.details = details
	}
}

func WithSeparator(separator string) option {
	return func(c *config) {
		c.separator = separator
	}
}

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		panic(err)
	}
	return u.Query().Get("node_id")
}
