package scope

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/trace"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

type callScope struct {
	config  config.Config
	latency registry.TimerVec
	calls   registry.GaugeVec
	errs    registry.GaugeVec
	value   interface{} // TODO: go1.18: GaugeVec or HistogramVec
}

func (s *callScope) RecordValue(tags map[string]string, value float64) {
	switch s.config.ValueType() {
	case config.ValueTypeGauge:
		s.value.(registry.GaugeVec).With(tags).Set(value)
	case config.ValueTypeHistogram:
		s.value.(registry.HistogramVec).With(tags).Record(value)
	default:
		// nop
	}
}

func (s *callScope) Start(lbls ...labels.Label) trace.Trace {
	s.calls.With(labels.KeyValue(
		append([]labels.Label{trace.Version, {
			Tag:   labels.TagSuccess,
			Value: "wip",
		}}, lbls...)...,
	)).Add(1)
	return trace.New(s)
}

func (s *callScope) AddCall(tags map[string]string, delta int) {
	if s.calls == nil {
		return
	}
	s.calls.With(tags).Add(float64(delta))
}

func (s *callScope) AddError(tags map[string]string) {
	if s.errs == nil {
		return
	}
	s.errs.With(tags).Add(1)
}

func (s *callScope) RecordLatency(tags map[string]string, latency time.Duration) {
	if s.latency == nil {
		return
	}
	s.latency.With(tags).Record(latency)
}

func New(c registry.Config, scope string, cfg config.Config, tags ...string) *callScope {
	c = c.WithSystem(scope)
	s := &callScope{
		config: cfg,
	}
	if cfg.HasCalls() {
		s.calls = c.GaugeVec("calls", append([]string{labels.TagSuccess, labels.TagVersion}, tags...)...)
	}
	if cfg.HasLatency() {
		s.latency = c.TimerVec("latency", append([]string{labels.TagSuccess, labels.TagVersion}, tags...)...)
	}
	if cfg.HasError() {
		s.errs = c.GaugeVec("errors", append([]string{labels.TagVersion, labels.TagError, labels.TagErrCode}, tags...)...)
	}
	switch cfg.ValueType() {
	case config.ValueTypeGauge:
		s.value = c.GaugeVec("value", append([]string{labels.TagVersion}, tags...)...)
	case config.ValueTypeHistogram:
		s.value = c.HistogramVec("value", append([]string{labels.TagVersion}, tags...)...)
	default:
		// nop
	}
	return s
}
