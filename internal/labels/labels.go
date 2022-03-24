package labels

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type Label struct {
	Tag   string
	Value string
}

const (
	TagVersion    = "sdk"
	TagSource     = "source"
	TagName       = "name"
	TagMethod     = "method"
	TagError      = "error"
	TagErrCode    = "errCode"
	TagAddress    = "address"
	TagID         = "ID"
	TagNodeID     = "nodeID"
	TagDataCenter = "destination"
	TagState      = "state"
	TagIdempotent = "idempotent"
	TagSuccess    = "success"
	TagStage      = "stage"
)

func KeyValue(labels ...Label) map[string]string {
	kv := make(map[string]string, len(labels))
	for _, l := range labels {
		kv[l.Tag] = l.Value
	}
	return kv
}

func Err(err error, lbls ...Label) []Label {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "network/" + netErr.Op + " -> " + netErr.Err.Error(),
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, io.EOF) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "io/EOF",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "context/DeadlineExceeded",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if errors.Is(err, context.Canceled) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "context/Canceled",
			},
			Label{
				Tag:   TagErrCode,
				Value: "-1",
			},
		)
	}
	if te := ydb.TransportError(err); te != nil {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "transport/" + te.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", te.Code()),
			},
		)
	}
	if oe := ydb.OperationError(err); oe != nil {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: "operation/" + oe.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: fmt.Sprintf("%06d", oe.Code()),
			},
		)
	}
	var e ydb.Error
	if errors.As(err, &e) {
		return append(
			lbls,
			Label{
				Tag:   TagError,
				Value: e.Name(),
			},
			Label{
				Tag:   TagErrCode,
				Value: strconv.Itoa(int(e.Code())),
			},
		)
	}
	return append(
		lbls,
		Label{
			Tag:   TagError,
			Value: "unknown",
		},
		Label{
			Tag:   TagErrCode,
			Value: "-1",
		},
	)
}
