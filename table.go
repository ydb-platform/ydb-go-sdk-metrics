package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Table(c Config) (t trace.Table) {
	c = c.WithSystem("table")
	if c.Details()&trace.TablePoolRetryEvents != 0 {
		do := metrics(c, "do", TagIdempotent, TagStage)
		doTx := metrics(c, "do_tx", TagIdempotent, TagStage)
		t.OnPoolDo = func(info trace.PoolDoStartInfo) func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
			idempotent := Label{
				Tag: TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			start := do.start(idempotent, Label{
				Tag:   TagStage,
				Value: "init",
			})
			return func(info trace.PoolDoIntermediateInfo) func(trace.PoolDoDoneInfo) {
				start.sync(info.Error, idempotent, Label{
					Tag:   TagStage,
					Value: "intermediate",
				})
				return func(info trace.PoolDoDoneInfo) {
					start.syncWithValue(info.Error, float64(info.Attempts), idempotent, Label{
						Tag:   TagStage,
						Value: "finish",
					})
				}
			}
		}
		t.OnPoolDoTx = func(info trace.PoolDoTxStartInfo) func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
			idempotent := Label{
				Tag: TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			start := doTx.start(idempotent, Label{
				Tag:   TagStage,
				Value: "init",
			})
			return func(info trace.PoolDoTxIntermediateInfo) func(trace.PoolDoTxDoneInfo) {
				start.sync(info.Error, idempotent, Label{
					Tag:   TagStage,
					Value: "intermediate",
				})
				return func(info trace.PoolDoTxDoneInfo) {
					start.syncWithValue(info.Error, float64(info.Attempts), idempotent, Label{
						Tag:   TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	if c.Details()&trace.TableSessionEvents != 0 {
		c := c.WithSystem("session")
		if c.Details()&trace.TableSessionLifeCycleEvents != 0 {
			new := metrics(c, "new", TagNodeID)
			delete := metrics(c, "delete", TagNodeID)
			keepAlive := metrics(c, "keep_alive", TagNodeID)
			t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
				start := new.start(Label{
					Tag:   TagNodeID,
					Value: "wip",
				})
				return func(info trace.SessionNewDoneInfo) {
					nodeID := Label{
						Tag: TagNodeID,
						Value: func() string {
							if info.Session != nil {
								return nodeID(info.Session.ID())
							}
							return ""
						}(),
					}
					lables, _ := start.sync(info.Error, nodeID)
					// publish empty delete call metric for register metrics on metrics storage
					delete.calls.With(labelsToKeyValue(lables...)).Add(0)
				}
			}
			t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := delete.start(nodeID)
				return func(info trace.SessionDeleteDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := keepAlive.start(nodeID)
				return func(info trace.KeepAliveDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
		}
		if c.Details()&trace.TableSessionQueryEvents != 0 {
			c := c.WithSystem("query")
			if c.Details()&trace.TableSessionQueryInvokeEvents != 0 {
				c := c.WithSystem("invoke")
				prepare := metrics(c, "prepare", TagNodeID)
				execute := metrics(c, "execute", TagNodeID)
				t.OnSessionQueryPrepare = func(
					info trace.PrepareDataQueryStartInfo,
				) func(
					trace.PrepareDataQueryDoneInfo,
				) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := prepare.start(nodeID)
					return func(info trace.PrepareDataQueryDoneInfo) {
						start.sync(info.Error, nodeID)
					}
				}
				t.OnSessionQueryExecute = func(
					info trace.ExecuteDataQueryStartInfo,
				) func(
					trace.ExecuteDataQueryDoneInfo,
				) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := execute.start(nodeID)
					return func(info trace.ExecuteDataQueryDoneInfo) {
						start.sync(info.Error, nodeID)
					}
				}
			}
			if c.Details()&trace.TableSessionQueryStreamEvents != 0 {
				c := c.WithSystem("stream")
				read := metrics(c, "read", TagStage, TagNodeID)
				execute := metrics(c, "execute", TagStage, TagNodeID)
				t.OnSessionQueryStreamExecute = func(
					info trace.SessionQueryStreamExecuteStartInfo,
				) func(
					trace.SessionQueryStreamExecuteIntermediateInfo,
				) func(
					trace.SessionQueryStreamExecuteDoneInfo,
				) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := execute.start(nodeID, Label{
						Tag:   TagStage,
						Value: "init",
					})
					return func(
						info trace.SessionQueryStreamExecuteIntermediateInfo,
					) func(
						trace.SessionQueryStreamExecuteDoneInfo,
					) {
						start.sync(info.Error, nodeID, Label{
							Tag:   TagStage,
							Value: "intermediate",
						})
						return func(info trace.SessionQueryStreamExecuteDoneInfo) {
							start.sync(info.Error, nodeID, Label{
								Tag:   TagStage,
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
					nodeID := Label{
						Tag:   TagNodeID,
						Value: nodeID(info.Session.ID()),
					}
					start := read.start(nodeID, Label{
						Tag:   TagStage,
						Value: "init",
					})
					return func(
						info trace.SessionQueryStreamReadIntermediateInfo,
					) func(
						trace.SessionQueryStreamReadDoneInfo,
					) {
						start.sync(info.Error, nodeID, Label{
							Tag:   TagStage,
							Value: "intermediate",
						})
						return func(info trace.SessionQueryStreamReadDoneInfo) {
							start.sync(info.Error, nodeID, Label{
								Tag:   TagStage,
								Value: "finish",
							})
						}
					}
				}
			}
		}
		if c.Details()&trace.TableSessionTransactionEvents != 0 {
			c := c.WithSystem("transaction")
			begin := metrics(c, "begin", TagNodeID)
			commit := metrics(c, "commit", TagNodeID)
			rollback := metrics(c, "rollback", TagNodeID)
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := begin.start(nodeID)
				return func(info trace.SessionTransactionBeginDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := commit.start(nodeID)
				return func(info trace.SessionTransactionCommitDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: nodeID(info.Session.ID()),
				}
				start := rollback.start(nodeID)
				return func(info trace.SessionTransactionRollbackDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
		}
	}
	if c.Details()&trace.TablePoolEvents != 0 {
		c := c.WithSystem("pool")
		if c.Details()&trace.TablePoolLifeCycleEvents != 0 {
			min := metrics(c, "min")
			max := metrics(c, "max")
			t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
				startMin := min.start()
				startMax := max.start()
				return func(info trace.PoolInitDoneInfo) {
					startMin.syncWithValue(nil, float64(info.KeepAliveMinSize))
					startMax.syncWithValue(nil, float64(info.Limit))
				}
			}
			t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
				startMin := min.start()
				startMax := max.start()
				return func(info trace.PoolCloseDoneInfo) {
					startMin.syncWithValue(info.Error, 0)
					startMax.syncWithValue(info.Error, 0)
				}
			}
		}
		if c.Details()&trace.TablePoolSessionLifeCycleEvents != 0 {
			c := c.WithSystem("session")
			new := metrics(c, "new")
			close := metrics(c, "close")
			t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				start := new.start()
				return func(info trace.PoolSessionNewDoneInfo) {
					lables, _ := start.sync(info.Error)
					// publish empty close call metric for register metrics on metrics storage
					close.calls.With(labelsToKeyValue(lables...)).Add(0)
				}
			}
			t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
				start := close.start()
				return func(info trace.PoolSessionCloseDoneInfo) {
					start.sync(nil)
				}
			}
		}
		if c.Details()&trace.TablePoolAPIEvents != 0 {
			put := metrics(c, "put", TagNodeID)
			get := metrics(c, "get", TagNodeID)
			wait := metrics(c, "wait", TagNodeID)
			take := metrics(c, "take", TagNodeID, TagStage)
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				nodeID := Label{
					Tag: TagNodeID,
					Value: func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return ""
					}(),
				}
				start := put.start(nodeID)
				return func(info trace.PoolPutDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
				node := Label{
					Tag:   TagNodeID,
					Value: "wip",
				}
				start := get.start(node)
				return func(info trace.PoolGetDoneInfo) {
					node.Value = func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return ""
					}()
					start.syncWithValue(info.Error, float64(info.Attempts), node)
				}
			}
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				node := Label{
					Tag:   TagNodeID,
					Value: "wip",
				}
				start := wait.start(node)
				return func(info trace.PoolWaitDoneInfo) {
					node.Value = func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return "-"
					}()
					start.sync(info.Error, node)
				}
			}
			t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(doneInfo trace.PoolTakeWaitInfo) func(doneInfo trace.PoolTakeDoneInfo) {
				nodeID := Label{
					Tag: TagNodeID,
					Value: func() string {
						if info.Session != nil {
							return nodeID(info.Session.ID())
						}
						return ""
					}(),
				}
				stage := Label{
					Tag:   TagStage,
					Value: "init",
				}
				start := take.start(nodeID)
				return func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
					stage.Value = "intermediate"
					start.sync(nil, nodeID, stage)
					return func(info trace.PoolTakeDoneInfo) {
						stage.Value = "finish"
						start.sync(info.Error, nodeID, stage)
					}
				}
			}
		}
	}
	return t
}
