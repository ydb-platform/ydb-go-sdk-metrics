package registry

// Counter counts value
type Counter interface {
	Inc()
}

// CounterVec returns Counter from CounterVec by labels
type CounterVec interface {
	With(map[string]string) Counter
}
