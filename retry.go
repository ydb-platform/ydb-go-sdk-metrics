package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

// Retry makes table.RetryTrace with New publishing
func Retry(c registry.Config) (t trace.Retry) {
	if c.Details()&trace.RetryEvents != 0 {
		retry := scope.New(c, "retry",
			config.New(
				config.WithValue(config.ValueTypeHistogram),
				config.WithValueBuckets([]float64{
					1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 50, 100, 200,
				}),
			),
			labels.TagIdempotent, labels.TagStage, labels.TagID,
		)
		t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			idempotent := labels.Label{
				Tag: labels.TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			id := labels.Label{
				Tag:   labels.TagID,
				Value: info.ID,
			}
			start := retry.Start(idempotent, id, labels.Label{
				Tag:   labels.TagStage,
				Value: "init",
			})
			return func(
				info trace.RetryLoopIntermediateInfo,
			) func(
				trace.RetryLoopDoneInfo,
			) {
				start.Sync(info.Error, idempotent, id, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.RetryLoopDoneInfo) {
					start.SyncWithValue(info.Error, float64(info.Attempts), idempotent, id, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	return t
}
