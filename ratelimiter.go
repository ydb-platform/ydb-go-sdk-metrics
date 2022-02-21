package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Ratelimiter(c registry.Config) (t trace.Ratelimiter) {
	return t
}
