package registry

// HistogramVec stores multiple dynamically created timers
type HistogramVec interface {
	With(map[string]string) Histogram
}

// Histogram tracks distribution of value.
type Histogram interface {
	Record(v float64)
}
