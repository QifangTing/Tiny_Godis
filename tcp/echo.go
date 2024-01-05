package tcp

/**
 * A echo server to test whether the server is functioning normally
 */

import (
	"Tiny_Godis/lib/logger"
	"Tiny_Godis/lib/sync/atomic"
	"Tiny_Godis/lib/sync/wait"
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"
)

// EchoHandler echos received line to client, using for test
type EchoHandler struct {
	activeConn sync.Map       // 记录连接
	closing    atomic.Boolean // 是否正在关闭，用原子布尔（避免并发竞争
}

// MakeEchoHandler creates EchoHandler
func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient is client for EchoHandler, using for test
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close close connection
func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

// Handle：处理客户端的连接
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() { // 当前正在关闭，不接受新连接
		_ = conn.Close()
	}

	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{}) // 存储新连接，k-v的v中用空结构体

	reader := bufio.NewReader(conn)
	for {
		// 使用缓存区接收用户发来的数据，使用\n作为结束
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b) // 返回接受的数据
		client.Waiting.Done()
	}
}

// Close stops echo handler
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}
