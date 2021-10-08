package metrics

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"strings"
	"sync"
)

type Name string

type Type int

const (
	NameError = Type(iota)
	NameUsed
	NameAttempts
	NameLatency
	NameMin
	NameMax
	NameTotal
	NameInProgress
	NameBalance
	NameInFlight
	NameStatus
	NameLocal
	NameSet
	NameIdempotent
	NameNonIdempotent
	NameNew
	NameDelete
	NameClose

	DriverName
	DriverNameConn
	DriverNameConnDial
	DriverNameConnInvoke
	DriverNameConnStream
	DriverNameConnStreamRecv
	DriverNameCluster
	DriverNamePessimize
	DriverNameInsert
	DriverNameUpdate
	DriverNameRemove
	DriverNameGet
	DriverNameGetCredentials
	DriverNameDiscovery
	DriverNameDiscoveryEndpoints

	TableName
	TableNameSession
	TableNameKeepAlive
	TableNameQuery
	TableNamePrepareData
	TableNameExecuteData
	TableNameStream
	TableNameStreamReadTable
	TableNameStreamExecuteScan
	TableNameTransaction
	TableNameBeginTransaction
	TableNameCommitTransaction
	TableNameRollbackTransaction
	TableNamePool
	TableNamePoolRetry
	TableNamePoolGet
	TableNamePoolWait
	TableNamePoolTake
	TableNamePoolPut
)

func defaultName(gaugeType Type) Name {
	switch gaugeType {
	case NameError:
		return "error"
	case NameUsed:
		return "used"
	case NameAttempts:
		return "attempts"
	case NameLatency:
		return "latency_ms"
	case NameMin:
		return "min"
	case NameMax:
		return "max"
	case NameTotal:
		return "total"
	case NameInProgress:
		return "in_progress"
	case NameInFlight:
		return "in_flight"
	case NameBalance:
		return "balance"
	case NameStatus:
		return "status"
	case NameLocal:
		return "local"
	case NameSet:
		return "set"
	case NameIdempotent:
		return "idempotent"
	case NameNonIdempotent:
		return "non-idempotent"
	case NameNew:
		return "new"
	case NameDelete:
		return "delete"
	case NameClose:
		return "close"

	case DriverName:
		return "driver"
	case DriverNameConn:
		return "conn"
	case DriverNameConnDial:
		return "dial"
	case DriverNameConnInvoke:
		return "invoke"
	case DriverNameConnStream:
		return "stream"
	case DriverNameConnStreamRecv:
		return "recv"
	case DriverNameCluster:
		return "cluster"
	case DriverNameInsert:
		return "insert"
	case DriverNameUpdate:
		return "update"
	case DriverNameRemove:
		return "remove"
	case DriverNameGet:
		return "get"
	case DriverNamePessimize:
		return "pessimize"
	case DriverNameGetCredentials:
		return "get_credentials"
	case DriverNameDiscovery:
		return "discovery"
	case DriverNameDiscoveryEndpoints:
		return "endpoints"

	case TableName:
		return "table"
	case TableNameSession:
		return "session"
	case TableNameKeepAlive:
		return "keep_alive"
	case TableNameQuery:
		return "query"
	case TableNamePrepareData:
		return "prepare_data"
	case TableNameExecuteData:
		return "execute_data"
	case TableNameStream:
		return "stream"
	case TableNameStreamReadTable:
		return "read_table"
	case TableNameStreamExecuteScan:
		return "execute_scan"
	case TableNameTransaction:
		return "transaction"
	case TableNameBeginTransaction:
		return "begin"
	case TableNameCommitTransaction:
		return "commit"
	case TableNameRollbackTransaction:
		return "rollback"
	case TableNamePool:
		return "pool"
	case TableNamePoolRetry:
		return "retry"
	case TableNamePoolGet:
		return "get"
	case TableNamePoolWait:
		return "wait"
	case TableNamePoolTake:
		return "take"
	case TableNamePoolPut:
		return "put"
	default:
		return ""
	}
}

type (
	nameFunc    func(gaugeType Type) Name
	errNameFunc func(err error) Name
	gaugeFunc   func(parts ...Name) Gauge
)

func parseConfig(c Config, scopes ...Type) (gaugeFunc, nameFunc, errNameFunc) {
	name := func(gaugeType Type) Name {
		if n := c.Name(gaugeType); n != nil {
			return Name(*n)
		}
		return defaultName(gaugeType)
	}
	prefix := make([]Name, 0, 1+len(scopes))
	if c.Prefix() != nil {
		prefix = append(prefix, Name(*(c.Prefix())))
	}
	for _, path := range scopes {
		prefix = append(prefix, name(path))
	}
	delimiter := "/"
	if c.Delimiter() != nil {
		delimiter = *c.Delimiter()
	}
	errName := func(err error) Name {
		if n := c.ErrName(err); n != nil {
			return Name(*n)
		}
		return Name(defaultErrName(err, delimiter))
	}
	gauges := make(map[Name]Gauge)
	mtx := sync.Mutex{}
	gauge := func(parts ...Name) Gauge {
		parts = append(prefix, parts...)
		n := c.Join(parts...)
		if n == nil {
			s := defaultJoin(delimiter, parts...)
			n = &s
		}
		mtx.Lock()
		defer mtx.Unlock()
		if gauge, ok := (gauges)[Name(*n)]; ok {
			return gauge
		}
		gauge := c.Gauge(*n)
		(gauges)[Name(*n)] = gauge
		return gauge
	}
	return gauge, name, errName
}

func defaultJoin(delimiter string, parts ...Name) string {
	s := make([]string, 0, len(parts))
	for _, p := range parts {
		ss := strings.TrimSpace(string(p))
		if ss != "" {
			s = append(s, ss)
		}
	}
	return strings.Join(s, delimiter)
}

func defaultErrName(err error, delimiter string) string {
	if ydb.IsTimeoutError(err) {
		return "timeout"
	}
	if ok, _, text := ydb.IsTransportError(err); ok {
		return strings.Join([]string{"transport", text}, delimiter)
	}
	if ok, _, text := ydb.IsOperationError(err); ok {
		return strings.Join([]string{"operation", text}, delimiter)
	}
	return strings.ReplaceAll(err.Error(), " ", "_")
}

type Gauge interface {
	// Inc increments the counter by 1
	Inc()
	// Dec decrements the counter by 1
	Dec()
	// Set sets the Gauge to an arbitrary value.
	Set(value float64)
	// Value returns current value
	Value() float64
}

type Details int

type Config interface {
	Details() Details
	// Gauge makes Gauge by name
	Gauge(name string) Gauge
	// Delimiter returns delimiter
	Delimiter() *string
	// Prefix returns prefix for gauge or empty string
	Prefix() *string
	// Name returns string name by type
	Name(Type) *string
	// Join returns Name after concatenation
	Join(parts ...Name) *string
	// ErrName returns Name by error
	ErrName(err error) *string
}
