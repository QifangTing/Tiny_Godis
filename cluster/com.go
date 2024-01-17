package cluster

import (
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/lib/utils"
	"Tiny_Godis/resp/client"
	"Tiny_Godis/resp/reply"
	"context"
	"errors"
	"strconv"
)

/*
com.go: 与其他节点通信
执行模式:
	本地（自己执行
	转发（别人执行
	群发（所有节点执行
*/

// 从连接池拿一个连接
func (cluster *clusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}

	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

// 归还连接
func (cluster *clusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// 转发指令给其他客户端，发送指令之前需要先发一下选择的db
func (cluster *clusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}

	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}

	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()

	// 注意切换数据库
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// 指令广播给所有节点
func (cluster *clusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		relay := cluster.relay(node, c, args)
		result[node] = relay
	}
	return result
}
