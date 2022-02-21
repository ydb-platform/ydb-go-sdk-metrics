package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	config2 "github.com/ydb-platform/ydb-go-sdk-metrics/registry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"net/url"
)

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		panic(err)
	}
	return u.Query().Get("node_id")
}

func Table(c config2.Config) (t trace.Table) {
	c = c.WithSystem("table")
	if c.Details()&trace.TablePoolRetryEvents != 0 {
		do := scope.New(c, "do", config.New(config.WithValue(config.ValueTypeGauge)), labels.TagIdempotent, labels.TagStage)
		doTx := scope.New(c, "do_tx", config.New(config.WithValue(config.ValueTypeGauge)), labels.TagIdempotent, labels.TagStage)
		t.OnPoolDo = func(info trace.PoolDoStartInfo) func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
			idempotent := labels.Label{
				Tag: labels.TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			start := do.Start(idempotent, labels.Label{
				Tag:   labels.TagStage,
				Value: "init",
			})
			return func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
				start.Sync(info.Error, idempotent, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.PoolDoDoneInfo) {
					start.SyncWithValue(info.Error, float64(info.Attempts), idempotent, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
		t.OnPoolDoTx = func(info trace.PoolDoTxStartInfo) func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
			idempotent := labels.Label{
				Tag: labels.TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			start := doTx.Start(idempotent, labels.Label{
				Tag:   labels.TagStage,
				Value: "init",
			})
			return func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
				start.Sync(info.Error, idempotent, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.PoolDoTxDoneInfo) {
					start.SyncWithValue(info.Error, float64(info.Attempts), idempotent, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	if c.Details()&trace.TableSessionEvents != 0 {
		c := c.WithSystem("session")
		if c.Details()&trace.TableSessionLifeCycleEvents != 0 {
			new := scope.New(c, "new", config.New(), labels.TagNodeID)
			delete := scope.New(c, "delete", config.New(), labels.TagNodeID)
			keepAlive := scope.New(c, "keep_alive", config.New(), labels.TagNodeID)
			t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
				start := new.Start(labels.Label{
					Tag:   labels.TagNodeID,
					Value: "wip",
				})
				return func(info trace.SessionNewDoneInfo) {
					nodeID := labels.Label{
						Tag: labels.TagNodeID,
						Value: func() string {
							if info.Session != nil {
								return nodeID(info.Session.ID())
							}
							return ""
						}(),
					}
					lables, _ := start.Sync(info.Error, nodeID)
					// publish empty delete call metric for register New on New storage
					delete.AddCall(labels.KeyValue(lables...), 0)
				}
			}
			t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := delete.Start(nodeID)
				return func(info trace.SessionDeleteDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := keepAlive.Start(nodeID)
				return func(info trace.KeepAliveDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
		}
		if c.Details()&trace.TableSessionQueryEvents != 0 {
			c := c.WithSystem("query")
			if c.Details()&trace.TableSessionQueryInvokeEvents != 0 {
				c := c.WithSystem("invoke")
				prepare := scope.New(c, "prepare", config.New(), labels.TagNodeID)
				execute := scope.New(c, "execute", config.New(), labels.TagNodeID)
				t.OnSessionQueryPrepare = func(
					info trace.PrepareDataQueryStartInfo,
				) func(
					trace.PrepareDataQueryDoneInfo,
				) {
					nodeID := labels.Label{
						Tag:   labels.TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := prepare.Start(nodeID)
					return func(info trace.PrepareDataQueryDoneInfo) {
						start.Sync(info.Error, nodeID)
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.ExecuteDataQueryStartInfo,
				) func(
					trace.ExecuteDataQueryDoneInfo,
				) {
					nodeID := labels.Label{
						Tag:   labels.TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := execute.Start(nodeID)
					return func(info trace.ExecuteDataQueryDoneInfo) {
						start.Sync(info.Error, nodeID)
					}
				}
			}
			if c.Details()&trace.TableSessionQueryStreamEvents != 0 {
				c := c.WithSystem("stream")
				read := scope.New(c, "read", config.New(), labels.TagStage, labels.TagNodeID)
				execute := scope.New(c, "execute", config.New(), labels.TagStage, labels.TagNodeID)
				t.OnSessionQueryStreamExecute = func(
					info trace.SessionQueryStreamExecuteStartInfo,
				) func(
					trace.SessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.SessionQueryStreamExecuteDoneInfo,
				) {
					nodeID := labels.Label{
						Tag:   labels.TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := execute.Start(nodeID, labels.Label{
						Tag:   labels.TagStage,
						Value: "init",
					})
					return func(
						info trace.SessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.SessionQueryStreamExecuteDoneInfo,
					) {
						start.Sync(info.Error, nodeID, labels.Label{
							Tag:   labels.TagStage,
							Value: "intermediate",
						})
						return func(info trace.SessionQueryStreamExecuteDoneInfo) {
							start.Sync(info.Error, nodeID, labels.Label{
								Tag:   labels.TagStage,
								Value: "finish",
							})
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.SessionQueryStreamReadStartInfo,
				) func(
					trace.SessionQueryStreamReadIntermediateInfo,
				) func(
					trace.SessionQueryStreamReadDoneInfo,
				) {
					nodeID := labels.Label{
						Tag:   labels.TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := read.Start(nodeID, labels.Label{
						Tag:   labels.TagStage,
						Value: "init",
					})
					return func(
						info trace.SessionQueryStreamReadIntermediateInfo,
					) func(
						trace.SessionQueryStreamReadDoneInfo,
					) {
						start.Sync(info.Error, nodeID, labels.Label{
							Tag:   labels.TagStage,
							Value: "intermediate",
						})
						return func(info trace.SessionQueryStreamReadDoneInfo) {
							start.Sync(info.Error, nodeID, labels.Label{
								Tag:   labels.TagStage,
								Value: "finish",
							})
						}
					}
				}
			}
		}
		if c.Details()&trace.TableSessionTransactionEvents != 0 {
			c := c.WithSystem("transaction")
			begin := scope.New(c, "begin", config.New(), labels.TagNodeID)
			commit := scope.New(c, "commit", config.New(), labels.TagNodeID)
			rollback := scope.New(c, "rollback", config.New(), labels.TagNodeID)
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := begin.Start(nodeID)
				return func(info trace.SessionTransactionBeginDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := commit.Start(nodeID)
				return func(info trace.SessionTransactionCommitDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := rollback.Start(nodeID)
				return func(info trace.SessionTransactionRollbackDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
		}
	}
	if c.Details()&trace.TablePoolEvents != 0 {
		c := c.WithSystem("pool")
		if c.Details()&trace.TablePoolLifeCycleEvents != 0 {
			min := scope.New(c, "min", config.New(
				config.WithoutCalls(),
				config.WithoutLatency(),
				config.WithValue(config.ValueTypeGauge)),
			)
			max := scope.New(c, "max", config.New(
				config.WithoutCalls(),
				config.WithoutLatency(),
				config.WithValue(config.ValueTypeGauge)),
			)
			t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
				startMin := min.Start()
				startMax := max.Start()
				return func(info trace.PoolInitDoneInfo) {
					startMin.SyncValue(float64(info.KeepAliveMinSize))
					startMax.SyncValue(float64(info.Limit))
				}
			}
			t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
				startMin := min.Start()
				startMax := max.Start()
				return func(info trace.PoolCloseDoneInfo) {
					startMin.SyncWithValue(info.Error, 0)
					startMax.SyncWithValue(info.Error, 0)
				}
			}
		}
		if c.Details()&trace.TablePoolSessionLifeCycleEvents != 0 {
			c := c.WithSystem("session")
			new := scope.New(c, "new", config.New())
			close := scope.New(c, "close", config.New())
			size := scope.New(c, "size", config.New())
			t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				start := new.Start()
				return func(info trace.PoolSessionNewDoneInfo) {
					lables, _ := start.Sync(info.Error)
					// publish empty close call metric for register New on New storage
					close.AddCall(labels.KeyValue(lables...), 0)
				}
			}
			t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
				start := close.Start()
				return func(info trace.PoolSessionCloseDoneInfo) {
					start.Sync(nil)
				}
			}
			t.OnPoolStateChange = func(info trace.PooStateChangeInfo) {
				size.RecordValue(nil, float64(info.Size))
			}
		}
		if c.Details()&trace.TablePoolAPIEvents != 0 {
			put := scope.New(c, "put", config.New(), labels.TagNodeID)
			get := scope.New(c, "get", config.New(), labels.TagNodeID)
			wait := scope.New(c, "wait", config.New(), labels.TagNodeID)
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				nodeID := labels.Label{
					Tag: labels.TagNodeID,
					Value: func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return ""
					}(),
				}
				start := put.Start(nodeID)
				return func(info trace.PoolPutDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
				node := labels.Label{
					Tag:   labels.TagNodeID,
					Value: "wip",
				}
				start := get.Start(node)
				return func(info trace.PoolGetDoneInfo) {
					node.Value = func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return ""
					}()
					start.SyncWithValue(info.Error, float64(info.Attempts), node)
				}
			}
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				node := labels.Label{
					Tag:   labels.TagNodeID,
					Value: "wip",
				}
				start := wait.Start(node)
				return func(info trace.PoolWaitDoneInfo) {
					node.Value = func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return "-"
					}()
					start.Sync(info.Error, node)
				}
			}
		}
	}
	return t
}
