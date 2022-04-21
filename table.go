package metrics

import (
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/labels"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
)

func nodeID(sessionID string) string {
	u, err := url.Parse(sessionID)
	if err != nil {
		panic(err)
	}
	return u.Query().Get("node_id")
}

func Table(c registry.Config) (t trace.Table) {
	c = c.WithSystem("table")
	if c.Details()&trace.TableEvents != 0 {
		createSession := scope.New(c, "createSession", config.New(
			config.WithValue(config.ValueTypeGauge)),
			labels.TagState,
		)
		t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
			start := createSession.Start(labels.Label{
				Tag:   labels.TagStage,
				Value: "start",
			})
			return func(info trace.TableCreateSessionIntermediateInfo) func(trace.TableCreateSessionDoneInfo) {
				start.Sync(info.Error, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.TableCreateSessionDoneInfo) {
					start.SyncWithValue(info.Error, float64(info.Attempts), labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
		do := scope.New(c, "do", config.New(
			config.WithValue(config.ValueTypeHistogram),
			config.WithValueBuckets([]float64{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 50, 100, 200,
			}),
		), labels.TagIdempotent, labels.TagStage)
		t.OnDo = func(info trace.TableDoStartInfo) func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
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
			return func(info trace.TableDoIntermediateInfo) func(trace.TableDoDoneInfo) {
				start.Sync(info.Error, idempotent, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.TableDoDoneInfo) {
					start.SyncWithValue(info.Error, float64(info.Attempts), idempotent, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
		doTx := scope.New(c, "do_tx", config.New(
			config.WithValue(config.ValueTypeHistogram),
			config.WithValueBuckets([]float64{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 50, 100, 200,
			}),
		), labels.TagIdempotent, labels.TagStage)
		t.OnDoTx = func(info trace.TableDoTxStartInfo) func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
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
			return func(info trace.TableDoTxIntermediateInfo) func(trace.TableDoTxDoneInfo) {
				start.Sync(info.Error, idempotent, labels.Label{
					Tag:   labels.TagStage,
					Value: "intermediate",
				})
				return func(info trace.TableDoTxDoneInfo) {
					start.SyncWithValue(info.Error, float64(info.Attempts), idempotent, labels.Label{
						Tag:   labels.TagStage,
						Value: "finish",
					})
				}
			}
		}
		{
			c := c.WithSystem("pool")
			min := scope.New(c, "min", config.New(
				config.WithValueOnly(config.ValueTypeGauge)),
			)
			max := scope.New(c, "max", config.New(
				config.WithValueOnly(config.ValueTypeGauge)),
			)
			t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
				return func(info trace.TableInitDoneInfo) {
					min.Start().SyncValue(float64(info.KeepAliveMinSize))
					max.Start().SyncValue(float64(info.Limit))
				}
			}
			t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
				return func(info trace.TableCloseDoneInfo) {
					min.Start().SyncValue(0)
					max.Start().SyncValue(0)
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
			t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
				start := new.Start(labels.Label{
					Tag:   labels.TagNodeID,
					Value: "wip",
				})
				return func(info trace.TableSessionNewDoneInfo) {
					nodeID := labels.Label{
						Tag: labels.TagNodeID,
						Value: func() string {
							if info.Session != nil {
								return nodeID(info.Session.ID())
							}
							return ""
						}(),
					}
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := delete.Start(nodeID)
				return func(info trace.TableSessionDeleteDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := keepAlive.Start(nodeID)
				return func(info trace.TableKeepAliveDoneInfo) {
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
					info trace.TablePrepareDataQueryStartInfo,
				) func(
					trace.TablePrepareDataQueryDoneInfo,
				) {
					nodeID := labels.Label{
						Tag:   labels.TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := prepare.Start(nodeID)
					return func(info trace.TablePrepareDataQueryDoneInfo) {
						start.Sync(info.Error, nodeID)
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.TableExecuteDataQueryStartInfo,
				) func(
					trace.TableExecuteDataQueryDoneInfo,
				) {
					nodeID := labels.Label{
						Tag:   labels.TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := execute.Start(nodeID)
					return func(info trace.TableExecuteDataQueryDoneInfo) {
						start.Sync(info.Error, nodeID)
					}
				}
			}
			if c.Details()&trace.TableSessionQueryStreamEvents != 0 {
				c := c.WithSystem("stream")
				read := scope.New(c, "read", config.New(), labels.TagStage, labels.TagNodeID)
				execute := scope.New(c, "execute", config.New(), labels.TagStage, labels.TagNodeID)
				t.OnSessionQueryStreamExecute = func(
					info trace.TableSessionQueryStreamExecuteStartInfo,
				) func(
					trace.TableSessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamExecuteDoneInfo,
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
						info trace.TableSessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamExecuteDoneInfo,
					) {
						start.Sync(info.Error, nodeID, labels.Label{
							Tag:   labels.TagStage,
							Value: "intermediate",
						})
						return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
							start.Sync(info.Error, nodeID, labels.Label{
								Tag:   labels.TagStage,
								Value: "finish",
							})
						}
					}
				}
				t.OnSessionQueryStreamRead = func(
					info trace.TableSessionQueryStreamReadStartInfo,
				) func(
					trace.TableSessionQueryStreamReadIntermediateInfo,
				) func(
					trace.TableSessionQueryStreamReadDoneInfo,
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
						info trace.TableSessionQueryStreamReadIntermediateInfo,
					) func(
						trace.TableSessionQueryStreamReadDoneInfo,
					) {
						start.Sync(info.Error, nodeID, labels.Label{
							Tag:   labels.TagStage,
							Value: "intermediate",
						})
						return func(info trace.TableSessionQueryStreamReadDoneInfo) {
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
			t.OnSessionTransactionBegin = func(info trace.TableSessionTransactionBeginStartInfo) func(trace.TableSessionTransactionBeginDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := begin.Start(nodeID)
				return func(info trace.TableSessionTransactionBeginDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.TableSessionTransactionCommitStartInfo) func(trace.TableSessionTransactionCommitDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := commit.Start(nodeID)
				return func(info trace.TableSessionTransactionCommitDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.TableSessionTransactionRollbackStartInfo) func(trace.TableSessionTransactionRollbackDoneInfo) {
				nodeID := labels.Label{
					Tag:   labels.TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := rollback.Start(nodeID)
				return func(info trace.TableSessionTransactionRollbackDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
		}
	}
	if c.Details()&trace.TablePoolEvents != 0 {
		c := c.WithSystem("pool")
		if c.Details()&trace.TablePoolLifeCycleEvents != 0 {
			size := scope.New(c, "size", config.New(
				config.WithValueOnly(config.ValueTypeGauge),
			))
			t.OnPoolStateChange = func(info trace.TablePooStateChangeInfo) {
				size.Start().SyncValue(float64(info.Size))
			}
		}
		if c.Details()&trace.TablePoolSessionLifeCycleEvents != 0 {
			c := c.WithSystem("session")
			new := scope.New(c, "new", config.New())
			close := scope.New(c, "close", config.New())
			t.OnPoolSessionNew = func(info trace.TablePoolSessionNewStartInfo) func(trace.TablePoolSessionNewDoneInfo) {
				start := new.Start()
				return func(info trace.TablePoolSessionNewDoneInfo) {
					start.Sync(info.Error)
				}
			}
			t.OnPoolSessionClose = func(info trace.TablePoolSessionCloseStartInfo) func(trace.TablePoolSessionCloseDoneInfo) {
				start := close.Start()
				return func(info trace.TablePoolSessionCloseDoneInfo) {
					start.Sync(nil)
				}
			}
		}
		if c.Details()&trace.TablePoolAPIEvents != 0 {
			put := scope.New(c, "put", config.New(), labels.TagNodeID)
			get := scope.New(c, "get", config.New(), labels.TagNodeID)
			wait := scope.New(c, "wait", config.New(), labels.TagNodeID)
			t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
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
				return func(info trace.TablePoolPutDoneInfo) {
					start.Sync(info.Error, nodeID)
				}
			}
			t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				node := labels.Label{
					Tag:   labels.TagNodeID,
					Value: "wip",
				}
				start := get.Start(node)
				return func(info trace.TablePoolGetDoneInfo) {
					node.Value = func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return ""
					}()
					start.SyncWithValue(info.Error, float64(info.Attempts), node)
				}
			}
			t.OnPoolWait = func(info trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
				node := labels.Label{
					Tag:   labels.TagNodeID,
					Value: "wip",
				}
				start := wait.Start(node)
				return func(info trace.TablePoolWaitDoneInfo) {
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
