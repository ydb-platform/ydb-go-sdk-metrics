package sensors

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Table(c Config) trace.Table {
	c = c.WithSystem("table")
	t := trace.Table{}
	if c.Details()&tablePoolRetryEvents != 0 {
		c := c.WithSystem("retry")
		latency := c.GaugeVec("latency", "latency of retry call", TagIdempotent, TagSuccess, TagVersion)
		attempts := c.GaugeVec("attempts", "attempts per retry call", TagIdempotent, TagSuccess, TagVersion)
		errs := c.GaugeVec("errors", "retry result errors", TagIdempotent, TagInternal, TagError, TagErrCode, TagVersion)
		calls := c.GaugeVec("calls", "retry calls in flight", TagIdempotent, TagVersion)
		t.OnPoolRetry = func(info trace.PoolRetryStartInfo) func(info trace.PoolRetryInternalInfo) func(trace.PoolRetryDoneInfo) {
			idempotent := Label{
				Tag: TagIdempotent,
				Value: func() string {
					if info.Idempotent {
						return "on"
					}
					return "off"
				}(),
			}
			calls.With(idempotent, version).Add(1)
			start := time.Now()
			return func(info trace.PoolRetryInternalInfo) func(trace.PoolRetryDoneInfo) {
				if info.Error != nil {
					errs.With(err(info.Error, idempotent, version, Label{Tag: TagInternal, Value: "true"})...).Add(1)
				}
				return func(info trace.PoolRetryDoneInfo) {
					calls.With(idempotent, version).Add(-1)
					success := Label{
						Tag: TagSuccess,
						Value: func() string {
							if info.Error == nil {
								return "true"
							}
							return "false"
						}(),
					}
					attempts.With(idempotent, success, version).Set(float64(info.Attempts))
					latency.With(idempotent, success, version).Set(float64(time.Since(start).Nanoseconds()))
					if info.Error != nil {
						errs.With(err(info.Error, idempotent, version, Label{Tag: TagInternal, Value: "false"})...).Add(1)
					}
				}
			}
		}
	}
	if c.Details()&TableSessionEvents != 0 {
		c := c.WithSystem("session")
		balance := c.GaugeVec("balance", "balance counter between created and deleted sessions", TagVersion, TagAddress)
		new := c.GaugeVec("new", "create sessions in flight", TagVersion, TagSuccess, TagAddress)
		delete := c.GaugeVec("delete", "delete sessions in flight", TagVersion, TagSuccess, TagAddress)
		keepalive := c.GaugeVec("keepalive", "keep-alive sessions in flight", TagVersion, TagSuccess, TagAddress)
		latency := c.GaugeVec("latency", "latency of call", TagSuccess, TagVersion, TagCall, TagAddress)
		errs := c.GaugeVec("errors", "session result errors", TagError, TagErrCode, TagVersion, TagCall)
		t.OnSessionNew = func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
			new.With(
				version,
				Label{
					Tag:   TagAddress,
					Value: "wip",
				},
				Label{
					Tag:   TagSuccess,
					Value: "wip",
				},
			).Add(1)
			start := time.Now()
			return func(info trace.SessionNewDoneInfo) {
				success := Label{
					Tag: TagSuccess,
					Value: func() string {
						if info.Error == nil {
							return "true"
						}
						return "false"
					}(),
				}
				address := Label{
					Tag: TagAddress,
					Value: func() string {
						if info.Session != nil {
							return info.Session.Address()
						}
						return ""
					}(),
				}
				latency.With(success, version, address, Label{Tag: TagCall, Value: "new"}).Set(float64(time.Since(start).Nanoseconds()))
				new.With(
					version,
					success,
					address,
				).Add(-1)
				if info.Error != nil {
					errs.With(err(info.Error, version, Label{Tag: TagCall, Value: "new"})...).Add(1)
				} else {
					balance.With(version, address).Add(1)
				}
			}
		}
		t.OnSessionDelete = func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
			address := Label{
				Tag:   TagAddress,
				Value: info.Session.Address(),
			}
			balance.With(version, address).Add(-1)
			delete.With(
				version,
				address,
				Label{
					Tag:   TagSuccess,
					Value: "wip",
				},
			).Add(1)
			start := time.Now()
			return func(info trace.SessionDeleteDoneInfo) {
				success := Label{
					Tag: TagSuccess,
					Value: func() string {
						if info.Error == nil {
							return "true"
						}
						return "false"
					}(),
				}
				latency.With(success, version, Label{Tag: TagCall, Value: "delete"}).Set(float64(time.Since(start).Nanoseconds()))
				delete.With(
					version,
					success,
					address,
				).Add(-1)
				if info.Error != nil {
					errs.With(err(info.Error, version, Label{Tag: TagCall, Value: "delete"})...).Add(1)
				}
			}
		}
		t.OnSessionKeepAlive = func(info trace.KeepAliveStartInfo) func(trace.KeepAliveDoneInfo) {
			keepalive.With(
				version,
				Label{
					Tag:   TagSuccess,
					Value: "wip",
				},
			).Add(1)
			start := time.Now()
			return func(info trace.KeepAliveDoneInfo) {
				success := Label{
					Tag: TagSuccess,
					Value: func() string {
						if info.Error == nil {
							return "true"
						}
						return "false"
					}(),
				}
				latency.With(success, version, Label{Tag: TagCall, Value: "keep-alive"}).Set(float64(time.Since(start).Nanoseconds()))
				keepalive.With(
					version,
					success,
				).Add(-1)
				if info.Error != nil {
					errs.With(err(info.Error, version, Label{Tag: TagCall, Value: "keep-alive"})...).Add(1)
				}
			}
		}
	}
	return t
}

// Table makes trace.ClientTrace with metrics publishing
//func Table(c Config) trace.Table {
//	t := trace.Table{}
//	if c.Details()&tableSessionQueryEvents != 0 {
//		t.OnSessionQueryPrepare = func(info trace.SessionQueryPrepareStartInfo) func(trace.PrepareDataQueryDoneInfo) {
//			start := time.Now()
//			return func(info trace.PrepareDataQueryDoneInfo) {
//				timer(
//					name(TableNameQuery),
//					name(TableNameData),
//					name(TableNameDataPrepare),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameQuery),
//					name(TableNameData),
//					name(TableNameDataPrepare),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameQuery),
//						name(TableNameData),
//						name(TableNameDataPrepare),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//		t.OnSessionQueryExecute = func(info trace.ExecuteDataQueryStartInfo) func(trace.SessionQueryPrepareDoneInfo) {
//			start := time.Now()
//			return func(info trace.SessionQueryPrepareDoneInfo) {
//				timer(
//					name(TableNameQuery),
//					name(TableNameData),
//					name(TableNameDataExecute),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameQuery),
//					name(TableNameData),
//					name(TableNameDataExecute),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameQuery),
//						name(TableNameData),
//						name(TableNameDataExecute),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//	}
//	if c.Details()&tableSessionQueryStreamEvents != 0 {
//		t.OnSessionQueryStreamExecute = func(info trace.SessionQueryStreamExecuteStartInfo) func(trace.SessionQueryStreamExecuteDoneInfo) {
//			start := time.Now()
//			return func(info trace.SessionQueryStreamExecuteDoneInfo) {
//				timer(
//					name(TableNameStream),
//					name(TableNameStreamExecuteScan),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameStream),
//					name(TableNameStreamExecuteScan),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameStream),
//						name(TableNameStreamExecuteScan),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//		t.OnSessionQueryStreamRead = func(info trace.SessionQueryStreamReadStartInfo) func(trace.SessionQueryStreamReadDoneInfo) {
//			start := time.Now()
//			return func(info trace.SessionQueryStreamReadDoneInfo) {
//				timer(
//					name(TableNameStream),
//					name(TableNameStreamReadTable),
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				c.Gauge(
//					name(TableNameStream),
//					name(TableNameStreamReadTable),
//				).Add(1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNameStream),
//						name(TableNameStreamReadTable),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
//			}
//		}
//	}
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
//				name(TableNameSession),
//				name(NameNew),
//				name(NameInProgress),
//			).Add(1)
//			return func(info trace.PoolSessionNewDoneInfo) {
//				c.Gauge(
//					name(TableNamePool),
//					name(TableNameSession),
//					name(NameNew),
//					name(NameInProgress),
//				).Add(-1)
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNameSession),
//						name(NameNew),
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				} else {
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNameSession),
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
//					name(TableNameSession),
//					name(NameClose),
//				).Add(1)
//				c.Gauge(
//					name(TableNamePool),
//					name(NameBalance),
//				).Add(-1)
//			}
//		}
//	}
//	if c.Details()&tablePoolRetryEvents != 0 {
//		t.OnPoolRetry = func(info trace.PoolRetryStartInfo) func(trace.PoolRetryDoneInfo) {
//			start := time.Now()
//			idempotent := func() Name {
//				if info.Idempotent {
//					return name(NameIdempotent)
//				}
//				return name(NameNonIdempotent)
//			}()
//			return func(info trace.PoolRetryDoneInfo) {
//				timer(
//					name(TableNamePool),
//					name(TableNamePoolRetry),
//					idempotent,
//					name(NameLatency),
//				).RecordDuration(time.Since(start))
//				gauge(
//					name(TableNamePool),
//					name(TableNamePoolRetry),
//					idempotent,
//					name(NameAttempts),
//				).Set(float64(info.Attempts))
//				if info.Error != nil {
//					c.Gauge(
//						name(TableNamePool),
//						name(TableNamePoolRetry),
//						idempotent,
//						name(NameError),
//						errName(info.Error),
//					).Add(1)
//				}
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
