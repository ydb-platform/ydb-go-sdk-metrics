package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"strconv"
)

func Table(c Config) trace.Table {
	c = c.WithSystem("table")
	t := trace.Table{}
	if c.Details()&tablePoolRetryEvents != 0 {
		retry := callGauges(c, "retry", TagIdempotent, TagStage)
		t.OnPoolRetry = func(info trace.PoolRetryStartInfo) func(info trace.PoolRetryInternalInfo) func(trace.PoolRetryDoneInfo) {
			idempotent := Label{
				Tag: TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "true"
					}
					return "false"
				}(),
			}
			start := retry.start(idempotent, Label{
				Tag:   TagStage,
				Value: "init",
			})
			return func(info trace.PoolRetryInternalInfo) func(trace.PoolRetryDoneInfo) {
				start.sync(info.Error, idempotent, Label{
					Tag:   TagStage,
					Value: "intermediate",
				})
				return func(info trace.PoolRetryDoneInfo) {
					start.syncWithValue(info.Error, float64(info.Attempts), idempotent, Label{
						Tag:   TagStage,
						Value: "finish",
					})
				}
			}
		}
	}
	if c.Details()&TableSessionEvents != 0 {
		c := c.WithSystem("session")
		if c.Details()&tableSessionEvents != 0 {
			new := callGauges(c, "new", TagNodeID)
			delete := callGauges(c, "delete", TagNodeID)
			keepAlive := callGauges(c, "keep_alive", TagNodeID)
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
								return strconv.FormatUint(uint64(info.Session.NodeID()), 10)
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
					Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
				}
				start := delete.start(nodeID)
				return func(info trace.SessionDeleteDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
				}
				start := keepAlive.start(nodeID)
				return func(info trace.KeepAliveDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
		}
		if c.Details()&tableSessionQueryEvents != 0 {
			c := c.WithSystem("query")
			if c.Details()&tableSessionQueryInvokeEvents != 0 {
				c := c.WithSystem("invoke")
				prepare := callGauges(c, "prepare", TagNodeID)
				execute := callGauges(c, "execute", TagNodeID)
				t.OnSessionQueryPrepare = func(info trace.SessionQueryPrepareStartInfo) func(trace.PrepareDataQueryDoneInfo) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
					}
					start := prepare.start(nodeID)
					return func(info trace.PrepareDataQueryDoneInfo) {
						start.sync(info.Error, nodeID)
					}
				}
				t.OnSessionQueryExecute = func(info trace.ExecuteDataQueryStartInfo) func(trace.SessionQueryPrepareDoneInfo) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
					}
					start := execute.start(nodeID)
					return func(info trace.SessionQueryPrepareDoneInfo) {
						start.sync(info.Error, nodeID)
					}
				}
			}
			if c.Details()&tableSessionQueryStreamEvents != 0 {
				c := c.WithSystem("stream")
				read := callGauges(c, "read", TagNodeID)
				execute := callGauges(c, "execute", TagNodeID)
				t.OnSessionQueryStreamExecute = func(info trace.SessionQueryStreamExecuteStartInfo) func(trace.SessionQueryStreamExecuteDoneInfo) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
					}
					start := execute.start(nodeID)
					return func(info trace.SessionQueryStreamExecuteDoneInfo) {
						start.sync(info.Error, nodeID)
					}
				}
				t.OnSessionQueryStreamRead = func(info trace.SessionQueryStreamReadStartInfo) func(trace.SessionQueryStreamReadDoneInfo) {
					nodeID := Label{
						Tag:   TagNodeID,
						Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
					}
					start := read.start(nodeID)
					return func(info trace.SessionQueryStreamReadDoneInfo) {
						start.sync(info.Error, nodeID)
					}
				}
			}
		}
		if c.Details()&tableSessionTransactionEvents != 0 {
			c := c.WithSystem("transaction")
			begin := callGauges(c, "begin", TagNodeID)
			commit := callGauges(c, "commit", TagNodeID)
			rollback := callGauges(c, "rollback", TagNodeID)
			t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
				}
				start := begin.start(nodeID)
				return func(info trace.SessionTransactionBeginDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
				}
				start := commit.start(nodeID)
				return func(info trace.SessionTransactionCommitDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
			t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: strconv.FormatUint(uint64(info.Session.NodeID()), 10),
				}
				start := rollback.start(nodeID)
				return func(info trace.SessionTransactionRollbackDoneInfo) {
					start.sync(info.Error, nodeID)
				}
			}
		}
	}
	if c.Details()&TablePoolEvents != 0 {
		c := c.WithSystem("pool")
		if c.Details()&tablePoolLifeCycleEvents != 0 {
			min := callGauges(c, "min")
			max := callGauges(c, "max")
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
		if c.Details()&tablePoolSessionLifeCycleEvents != 0 {
			c := c.WithSystem("session")
			new := callGauges(c, "new")
			close := callGauges(c, "close")
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
		if c.Details()&tablePoolAPIEvents != 0 {
			put := callGauges(c, "put", TagNodeID)
			get := callGauges(c, "get", TagNodeID)
			wait := callGauges(c, "wait", TagNodeID)
			take := callGauges(c, "take", TagNodeID, TagStage)
			t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				nodeID := Label{
					Tag: TagNodeID,
					Value: func() string {
						if info.Session != nil {
							return strconv.FormatUint(uint64(info.Session.NodeID()), 10)
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
				nodeID := Label{
					Tag:   TagNodeID,
					Value: "wip",
				}
				start := get.start(nodeID)
				return func(info trace.PoolGetDoneInfo) {
					nodeID.Value = func() string {
						if info.Session != nil {
							return strconv.FormatUint(uint64(info.Session.NodeID()), 10)
						}
						return ""
					}()
					start.syncWithValue(info.Error, float64(info.Attempts), nodeID)
				}
			}
			t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
				nodeID := Label{
					Tag:   TagNodeID,
					Value: "wip",
				}
				start := wait.start(nodeID)
				return func(info trace.PoolWaitDoneInfo) {
					nodeID.Value = func() string {
						if info.Session != nil {
							return strconv.FormatUint(uint64(info.Session.NodeID()), 10)
						}
						return ""
					}()
					start.sync(info.Error, nodeID)
				}
			}
			t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(doneInfo trace.PoolTakeWaitInfo) func(doneInfo trace.PoolTakeDoneInfo) {
				nodeID := Label{
					Tag: TagNodeID,
					Value: func() string {
						if info.Session != nil {
							return strconv.FormatUint(uint64(info.Session.NodeID()), 10)
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

// TableWithRegistry makes trace.Table with metrics registry and options
func TableWithRegistry(registry Registry, opts ...option) trace.Table {
	c := &config{
		registry:  registry,
		namespace: defaultNamespace,
		separator: defaultSeparator,
	}
	for _, o := range opts {
		o(c)
	}

	if c.details == 0 {
		c.details = ^Details(0)
	}
	return Table(c)
}
