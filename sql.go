package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope"
	"github.com/ydb-platform/ydb-go-sdk-metrics/internal/scope/config"
	"github.com/ydb-platform/ydb-go-sdk-metrics/registry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// DatabaseSQL makes trace.DatabaseSQL with measuring `database/sql` events
func DatabaseSQL(c registry.Config) (t trace.DatabaseSQL) {
	if c.Details()&trace.DatabaseSQLEvents == 0 {
		return t
	}
	c = c.WithSystem("database").WithSystem("sql")
	if c.Details()&trace.DatabaseSQLConnectorEvents != 0 {
		//nolint:govet
		c := c.WithSystem("connector")
		connect := scope.New(c, "connect", config.New())
		t.OnConnectorConnect = func(
			info trace.DatabaseSQLConnectorConnectStartInfo,
		) func(
			trace.DatabaseSQLConnectorConnectDoneInfo,
		) {
			start := connect.Start()
			return func(info trace.DatabaseSQLConnectorConnectDoneInfo) {
				start.Sync(info.Error)
			}
		}
	}
	//nolint:nestif
	if c.Details()&trace.DatabaseSQLConnEvents != 0 {
		//nolint:govet
		c := c.WithSystem("conn")
		ping := scope.New(c, "ping", config.New())
		close := scope.New(c, "close", config.New())
		begin := scope.New(c, "begin", config.New())
		prepare := scope.New(c, "prepare", config.New())
		exec := scope.New(c, "exec", config.New())
		query := scope.New(c, "query", config.New())
		t.OnConnPing = func(info trace.DatabaseSQLConnPingStartInfo) func(trace.DatabaseSQLConnPingDoneInfo) {
			start := ping.Start()
			return func(info trace.DatabaseSQLConnPingDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnConnClose = func(info trace.DatabaseSQLConnCloseStartInfo) func(trace.DatabaseSQLConnCloseDoneInfo) {
			start := close.Start()
			return func(info trace.DatabaseSQLConnCloseDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnConnBegin = func(info trace.DatabaseSQLConnBeginStartInfo) func(trace.DatabaseSQLConnBeginDoneInfo) {
			start := begin.Start()
			return func(info trace.DatabaseSQLConnBeginDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnConnPrepare = func(info trace.DatabaseSQLConnPrepareStartInfo) func(trace.DatabaseSQLConnPrepareDoneInfo) {
			start := prepare.Start()
			return func(info trace.DatabaseSQLConnPrepareDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnConnExec = func(info trace.DatabaseSQLConnExecStartInfo) func(trace.DatabaseSQLConnExecDoneInfo) {
			start := exec.Start()
			return func(info trace.DatabaseSQLConnExecDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnConnQuery = func(info trace.DatabaseSQLConnQueryStartInfo) func(trace.DatabaseSQLConnQueryDoneInfo) {
			start := query.Start()
			return func(info trace.DatabaseSQLConnQueryDoneInfo) {
				start.Sync(info.Error)
			}
		}
	}
	if c.Details()&trace.DatabaseSQLTxEvents != 0 {
		//nolint:govet
		c := c.WithSystem("tx")
		commit := scope.New(c, "commit", config.New())
		rollback := scope.New(c, "rollback", config.New())
		t.OnTxCommit = func(info trace.DatabaseSQLTxCommitStartInfo) func(trace.DatabaseSQLTxCommitDoneInfo) {
			start := commit.Start()
			return func(info trace.DatabaseSQLTxCommitDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnTxRollback = func(info trace.DatabaseSQLTxRollbackStartInfo) func(trace.DatabaseSQLTxRollbackDoneInfo) {
			start := rollback.Start()
			return func(info trace.DatabaseSQLTxRollbackDoneInfo) {
				start.Sync(info.Error)
			}
		}
	}
	//nolint:nestif
	if c.Details()&trace.DatabaseSQLStmtEvents != 0 {
		//nolint:govet
		c := c.WithSystem("stmt")
		close := scope.New(c, "close", config.New())
		exec := scope.New(c, "exec", config.New())
		query := scope.New(c, "query", config.New())
		t.OnStmtClose = func(info trace.DatabaseSQLStmtCloseStartInfo) func(trace.DatabaseSQLStmtCloseDoneInfo) {
			start := close.Start()
			return func(info trace.DatabaseSQLStmtCloseDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnStmtExec = func(info trace.DatabaseSQLStmtExecStartInfo) func(trace.DatabaseSQLStmtExecDoneInfo) {
			start := exec.Start()
			return func(info trace.DatabaseSQLStmtExecDoneInfo) {
				start.Sync(info.Error)
			}
		}
		t.OnStmtQuery = func(info trace.DatabaseSQLStmtQueryStartInfo) func(trace.DatabaseSQLStmtQueryDoneInfo) {
			start := query.Start()
			return func(info trace.DatabaseSQLStmtQueryDoneInfo) {
				start.Sync(info.Error)
			}
		}
	}
	return t
}
