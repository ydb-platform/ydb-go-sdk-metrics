package trace

import (
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/str"
)

var (
	Version = labels.Label{
		Tag: labels.TagVersion,
		Value: func() string {
			_, version := path.Split(ydb.Version)
			return version
		}(),
	}
)

type Trace interface {
	Sync(e error, lbls ...labels.Label)
	SyncValue(v float64, lbls ...labels.Label)
	SyncWithValue(err error, v float64, lbls ...labels.Label)
}

type Scope interface {
	Start(lbls ...labels.Label) Trace
	AddCall(tags map[string]string, calls int)
	AddError(tags map[string]string)
	RecordLatency(tags map[string]string, latency time.Duration)
	RecordValue(tags map[string]string, value float64)
}

type callTrace struct {
	scope Scope
	start time.Time
}

func New(s Scope) Trace {
	return &callTrace{
		scope: s,
		start: time.Now(),
	}
}

func (t *callTrace) syncValue(v float64, lbls ...labels.Label) {
	t.scope.RecordValue(labels.KeyValue(append([]labels.Label{Version}, lbls...)...), v)
}

func (t *callTrace) SyncValue(v float64, lbls ...labels.Label) {
	t.syncValue(v, lbls...)
}

func (t *callTrace) SyncWithValue(err error, v float64, lbls ...labels.Label) {
	t.syncError(err, lbls...)
	t.syncWithSuccess(err == nil, lbls...)
	t.syncValue(v, lbls...)
}

func (t *callTrace) syncWithSuccess(ok bool, lbls ...labels.Label) (callLabels []labels.Label) {
	success := labels.Label{
		Tag:   labels.TagSuccess,
		Value: str.If(ok, "true", "false"),
	}
	t.scope.AddCall(labels.KeyValue(append([]labels.Label{Version, success}, lbls...)...), 1)
	t.scope.RecordLatency(labels.KeyValue(append([]labels.Label{Version, success}, lbls...)...), time.Since(t.start))
	return append([]labels.Label{Version, success}, lbls...)
}

func (t *callTrace) syncError(err error, lbls ...labels.Label) {
	if err != nil {
		t.scope.AddError(labels.KeyValue(labels.Err(err, append([]labels.Label{Version}, lbls...)...)...))
	}
}

func (t *callTrace) Sync(err error, lbls ...labels.Label) {
	t.syncError(err, lbls...)
	t.syncWithSuccess(err == nil, lbls...)
}
