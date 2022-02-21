package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Scheme(c registry.Config) (t trace.Scheme) {
	return t
}
