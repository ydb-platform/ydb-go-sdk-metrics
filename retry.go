package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Retry makes table.RetryTrace with metrics publishing
func Retry(c Config) (t trace.Retry) {
	if c.Details()&trace.RetryEvents != 0 {
		retry := metrics(c, "retry", TagIdempotent, TagStage, TagID)
		t.OnRetry = func(
			info trace.RetryLoopStartInfo,
		) func(
			trace.RetryLoopIntermediateInfo,
		) func(
			trace.RetryLoopDoneInfo,
		) {
			idempotent := Label{
				Tag: TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			id := Label{
				Tag:   TagID,
				Value: info.ID,
			}
			start := retry.start(idempotent, id, Label{
				Tag:   TagStage,
				Value: "init",
			})
			return func(
				info trace.RetryLoopIntermediateInfo,
			) func(
				trace.RetryLoopDoneInfo,
			) {
				start.sync(info.Error, idempotent, id, Label{
					Tag:   TagStage,
					Value: "intermediate",
				})
				return func(info trace.RetryLoopDoneInfo) {
					start.syncWithValue(info.Error, float64(info.Attempts), idempotent, id, Label{
						Tag:   TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	return t
}
