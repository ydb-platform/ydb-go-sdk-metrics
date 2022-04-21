package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

func Scheme(c registry.Config) (t trace.Scheme) {
	return t
}
