package config

type ValueType uint8

const (
	ValueTypeNone = ValueType(iota)
	ValueTypeGauge
	ValueTypeHistogram
)

type Config interface {
	HasLatency() bool
	HasCalls() bool
	HasError() bool
	ValueType() ValueType
	ValueBuckets() []float64
}

type config struct {
	withLatency  bool
	withCalls    bool
	withError    bool
	withValue    ValueType
	valueBuckets []float64
}

func (c *config) ValueBuckets() []float64 {
	return c.valueBuckets
}

func (c *config) HasLatency() bool {
	return c.withLatency
}

func (c *config) HasCalls() bool {
	return c.withCalls
}

func (c *config) HasError() bool {
	return c.withError
}

func (c *config) ValueType() ValueType {
	return c.withValue
}

type option func(o *config)

func WithoutLatency() option {
	return func(o *config) {
		o.withLatency = false
	}
}

func WithoutCalls() option {
	return func(o *config) {
		o.withCalls = false
	}
}

func WithoutError() option {
	return func(o *config) {
		o.withError = false
	}
}

func WithValue(valueType ValueType) option {
	return func(o *config) {
		o.withValue = valueType
	}
}

func WithValueBuckets(buckets []float64) option {
	return func(o *config) {
		o.valueBuckets = buckets
	}
}

func New(opts ...option) Config {
	h := &config{
		withLatency:  true,
		withCalls:    true,
		withError:    true,
		withValue:    ValueTypeNone,
		valueBuckets: make([]float64, 0),
	}
	for _, o := range opts {
		o(h)
	}
	return h
}
