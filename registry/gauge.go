package registry

// Gauge tracks single float64 value.
type Gauge interface {
	Add(delta float64)
	Set(value float64)
}

// GaugeVec returns Gauge from GaugeVec by labels
type GaugeVec interface {
	With(map[string]string) Gauge
}
