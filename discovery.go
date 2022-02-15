package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Discovery(c Config) (t trace.Discovery) {
	if c.Details()&trace.DiscoveryEvents != 0 {
		discovery := metrics(c, "discovery")
		t.OnDiscover = func(info trace.DiscoverStartInfo) func(trace.DiscoverDoneInfo) {
			start := discovery.start()
			return func(info trace.DiscoverDoneInfo) {
				start.syncWithValue(info.Error, float64(len(info.Endpoints)))
			}
		}
	}
	return t
}
