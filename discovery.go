package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

func Discovery(c registry.Config) (t trace.Discovery) {
	if c.Details()&trace.DiscoveryEvents != 0 {
		discovery := scope.New(c, "discovery",
			config.New(
				config.WithValue(config.ValueTypeGauge),
			),
			labels.TagAddress,
		)
		t.OnDiscover = func(info trace.DiscoveryDiscoverStartInfo) func(trace.DiscoveryDiscoverDoneInfo) {
			address := labels.Label{
				Tag:   labels.TagAddress,
				Value: info.Address,
			}
			start := discovery.Start(address)
			return func(info trace.DiscoveryDiscoverDoneInfo) {
				start.SyncWithValue(info.Error, float64(len(info.Endpoints)), address)
			}
		}
	}
	return t
}
