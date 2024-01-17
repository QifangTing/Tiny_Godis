package handler

import (
	"Tiny_Godis/cluster"
	"Tiny_Godis/config"
	"Tiny_Godis/database"
	databaseface "Tiny_Godis/interface/database"
	"Tiny_Godis/lib/logger"
	"Tiny_Godis/lib/sync/atomic"
	"Tiny_Godis/resp/connection"
	"Tiny_Godis/resp/parser"
	"Tiny_Godis/resp/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknow\r\n")
)

// 在tcp/echo.go/EchoHandler的基础上
type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Boolean
}

func MakeHandler() *RespHandler {
	var db databaseface.Database
	// 判断是单机还是集群
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewDatabase()
	}

	return &RespHandler{
		db: db,
	}
}

// 关闭单个客户端连接
func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, 1)

	ch := parser.ParseStream(conn) // 交给parser处理
	for payload := range ch {
		if payload.Err != nil { //处理错误
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 需要关闭
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			} else { //协议错误，先尝试写回
				errReply := reply.MakeErrReply(payload.Err.Error())
				err := client.Write(errReply.ToBytes())
				if err != nil { // 回写还是出错，关闭
					h.closeClient(client)
					logger.Info("connection closed: " + client.RemoteAddr().String())
					return
				}
				continue
			}
		} else { //执行
			if payload.Data == nil {
				logger.Error("empty payload")
				continue
			}
			r, ok := payload.Data.(*reply.MultiBulkReply)
			if !ok {
				logger.Error("require multi bulk reply")
				continue
			}
			result := h.db.Exec(client, r.Args)
			if result != nil {
				_ = client.Write(result.ToBytes())
			} else {
				_ = client.Write(unknownErrReplyBytes)
			}
		}
	}
}

// 关闭协议
func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
