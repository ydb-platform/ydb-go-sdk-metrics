package metrics

import (
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
					start.sync(info.Error, Label{
						Tag:   TagNodeID,
						Value: func() string {
							if info.Session != nil {
								return strconv.FormatUint(uint64(info.Session.NodeID()), 10)
							}
							return ""
						}(),
					})
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
	}
	return t
}

//	if c.Details()&tableSessionTransactionEvents != 0 {
//		t.OnSessionTransactionBegin = func(info trace.SessionTransactionBeginStartInfo) func(trace.SessionTransactionBeginDoneInfo) {
//			start := time.Now()
//			return func(info trace.SessionTransactionBeginDoneInfo) {
//				timer(
//					name(TableNameTransaction),
//					name(TableNameTransactionBegin),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameTransaction),
//					name(TableNameTransactionBegin),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameTransaction),
//						name(TableNameTransactionBegin),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//		t.OnSessionTransactionCommit = func(info trace.SessionTransactionCommitStartInfo) func(trace.SessionTransactionCommitDoneInfo) {
//			start := time.Now()
//			return func(info trace.SessionTransactionCommitDoneInfo) {
//				timer(
//					name(TableNameTransaction),
//					name(TableNameTransactionCommit),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameTransaction),
//					name(TableNameTransactionCommit),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameTransaction),
//						name(TableNameTransactionCommit),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//		t.OnSessionTransactionRollback = func(info trace.SessionTransactionRollbackStartInfo) func(trace.SessionTransactionRollbackDoneInfo) {
//			start := time.Now()
//			return func(info trace.SessionTransactionRollbackDoneInfo) {
//				timer(
//					name(TableNameTransaction),
//					name(TableNameTransactionRollback),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameTransaction),
//					name(TableNameTransactionRollback),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameTransaction),
//						name(TableNameTransactionRollback),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//	}
//	if c.Details()&tablePoolLifeCycleEvents != 0 {
//		t.OnPoolInit = func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
//			return func(info trace.PoolInitDoneInfo) {
//				gauge(
//					name(TableNamePool),
//					name(NameMax),
//				).Set(float64(info.Limit))
//				gauge(
//					name(TableNamePool),
//					name(NameMin),
//				).Set(float64(info.KeepAliveMinSize))
//			}
//		}
//		t.OnPoolClose = func(info trace.PoolCloseStartInfo) func(trace.PoolCloseDoneInfo) {
//			return func(info trace.PoolCloseDoneInfo) {
//				gauge(
//					name(TableNamePool),
//					name(NameMax),
//				).Set(0)
//				gauge(
//					name(TableNamePool),
//					name(NameMin),
//				).Set(0)
//			}
//		}
//	}
//	if c.Details()&tablePoolSessionLifeCycleEvents != 0 {
//		t.OnPoolSessionNew = func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
//			c.Gauge(
//				name(TableNamePool),
//				name(Tablecallsession),
//				name(NameNew),
//				name(NameInProgress),
//			).Add(1)
//			return func(info trace.PoolSessionNewDoneInfo) {
//				c.Gauge(
//					name(TableNamePool),
//					name(Tablecallsession),
//					name(NameNew),
//					name(NameInProgress),
//				).Add(-1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNamePool),
//						name(Tablecallsession),
//						name(NameNew),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				} else {
//					c.Gauge(
//						name(TableNamePool),
//						name(Tablecallsession),
//						name(NameNew),
//					).Add(1)
//					c.Gauge(
//						name(TableNamePool),
//						name(NameBalance),
//					).Add(1)
//				}
//			}
//		}
//		t.OnPoolSessionClose = func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
//			return func(info trace.PoolSessionCloseDoneInfo) {
//				c.Gauge(
//					name(TableNamePool),
//					name(Tablecallsession),
//					name(NameClose),
//				).Add(1)
//				c.Gauge(
//					name(TableNamePool),
//					name(NameBalance),
//				).Add(-1)
//			}
//		}
//	}
//	if c.Details()&tablePoolCommonAPIEvents != 0 {
//		t.OnPoolPut = func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
//			return func(info trace.PoolPutDoneInfo) {
//				c.Gauge(
//					name(TableNamePool),
//					name(TableNamePoolPut),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNamePoolPut),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//	}
//	if c.Details()&tablePoolNativeAPIEvents != 0 {
//		t.OnPoolGet = func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
//			return func(info trace.PoolGetDoneInfo) {
//				c.Gauge(
//					name(TableNamePool),
//					name(TableNamePoolGet),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNamePoolGet),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//		t.OnPoolWait = func(info trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
//			c.Gauge(
//				name(TableNamePool),
//				name(TableNamePoolWait),
//				name(NameBalance),
//			).Add(1)
//			return func(info trace.PoolWaitDoneInfo) {
//				c.Gauge(
//					name(TableNamePool),
//					name(TableNamePoolWait),
//					name(NameBalance),
//				).Add(-1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNamePoolWait),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//	}
//	if c.Details()&tablePoolYdbSqlAPIEvents != 0 {
//		t.OnPoolTake = func(info trace.PoolTakeStartInfo) func(doneInfo trace.PoolTakeWaitInfo) func(doneInfo trace.PoolTakeDoneInfo) {
//			start := time.Now()
//			return func(info trace.PoolTakeWaitInfo) func(info trace.PoolTakeDoneInfo) {
//				return func(info trace.PoolTakeDoneInfo) {
//					timer(
//						name(TableNamePool),
//						name(TableNamePoolTake),
//						name(NameLatency),
//					).RecordDuration(time.Since(start))
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNamePoolTake),
//					).Add(1)
//					if info.Error != nil {
//						c.Gauge(
//							name(TableNamePool),
//							name(TableNamePoolTake),
//							name(NameError),
//							errName(info.Error),
//						).Add(1)
//					}
//				}
//			}
//		}
//	}
//	return t
//}
