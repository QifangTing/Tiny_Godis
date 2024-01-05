package tcp

import (
	"context"
	"net"
)

// 业务逻辑的处理接口
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
