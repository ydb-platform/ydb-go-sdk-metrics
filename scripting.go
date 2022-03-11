package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

func Scripting(c registry.Config) (t trace.Scripting) {
	if c.Details()&trace.ScriptingEvents != 0 {
		c := c.WithSystem("scripting")
		execute := scope.New(c, "execute", config.New())
		explain := scope.New(c, "explain", config.New())
		streamExecute := scope.New(c.WithSystem("stream"), "execute", config.New(), labels.TagStage)
		t.OnExecute = func(info trace.ScriptingExecuteStartInfo) func(trace.ScriptingExecuteDoneInfo) {
			start := execute.Start()
			return func(info trace.ScriptingExecuteDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnExplain = func(info trace.ScriptingExplainStartInfo) func(trace.ScriptingExplainDoneInfo) {
			start := explain.Start()
			return func(info trace.ScriptingExplainDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnStreamExecute = func(
			info trace.ScriptingStreamExecuteStartInfo,
		) func(
			trace.ScriptingStreamExecuteIntermediateInfo,
		) func(
			trace.ScriptingStreamExecuteDoneInfo,
		) {
			start := streamExecute.Start(labels.Label{
				Tag:   labels.TagStage,
				Value: "init",
			})
			return func(
				info trace.ScriptingStreamExecuteIntermediateInfo,
			) func(
				trace.ScriptingStreamExecuteDoneInfo,
			) {
				start.Sync(info.Error, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.ScriptingStreamExecuteDoneInfo) {
					start.Sync(info.Error, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	return t
}
