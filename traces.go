package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func WithTraces(c Config) ydb.Option {
	return ydb.MergeOptions(
		ydb.WithTraceDriver(Driver(c)),
		ydb.WithTraceTable(Table(c)),
		ydb.WithTraceScripting(Scripting(c)),
		ydb.WithTraceScheme(Scheme(c)),
		ydb.WithTraceCoordination(Coordination(c)),
		ydb.WithTraceRatelimiter(Ratelimiter(c)),
		ydb.WithTraceDiscovery(Discovery(c)),
	)
}
