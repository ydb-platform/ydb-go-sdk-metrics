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
	calls   registry.CounterVec
	errs    registry.CounterVec
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
	if s.config.HasCalls() {
		s.calls.With(labels.KeyValue(
			append([]labels.Label{trace.Version, {
				Tag:   labels.TagSuccess,
				Value: "wip",
			}}, lbls...)...,
		)).Inc()
	}
	return trace.New(s)
}

func (s *callScope) AddCall(tags map[string]string) {
	if s.config.HasCalls() {
		s.calls.With(tags).Inc()
	}
}

func (s *callScope) AddError(tags map[string]string) {
	if s.config.HasError() {
		s.errs.With(tags).Inc()
	}
}

func (s *callScope) RecordLatency(tags map[string]string, latency time.Duration) {
	if s.config.HasLatency() {
		s.latency.With(tags).Record(latency)
	}
}

func New(c registry.Config, name string, cfg config.Config, tags ...string) *callScope {
	c = c.WithSystem(name)
	s := &callScope{
		config: cfg,
	}

	if cfg.HasCalls() {
		s.calls = c.CounterVec("calls", append([]string{labels.TagSuccess, labels.TagVersion}, tags...)...)
	}

	if cfg.HasLatency() {
		s.latency = c.TimerVec("latency", append([]string{labels.TagSuccess, labels.TagVersion}, tags...)...)
	}

	if cfg.HasError() {
		s.errs = c.CounterVec("errors", append([]string{labels.TagVersion, labels.TagError, labels.TagErrCode}, tags...)...)
	}

	if cfg.ValueType() == config.ValueTypeNone {
		return s
	}

	tags = append(tags, labels.TagVersion)

	if cfg.HasError() {
		tags = append(tags, labels.TagSuccess)
	}

	switch cfg.ValueType() {
	case config.ValueTypeGauge:
		s.value = c.GaugeVec("value", tags...)
	case config.ValueTypeHistogram:
		s.value = c.HistogramVec("value", cfg.ValueBuckets(), tags...)
	default:
		// nop
	}
	return s
}
