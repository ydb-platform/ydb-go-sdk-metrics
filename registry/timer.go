package registry

import "time"

// TimerVec stores multiple dynamically created timers
type TimerVec interface {
	With(map[string]string) Timer
}

// Timer tracks distribution of value.
type Timer interface {
	Record(d time.Duration)
}
