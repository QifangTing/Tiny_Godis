package cluster

import (
	"Tiny_Godis/config"
	"Tiny_Godis/database"
	databaseface "Tiny_Godis/interface/database"
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/lib/consistenthash"
	"Tiny_Godis/lib/logger"
	"Tiny_Godis/resp/reply"
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	pool "github.com/jolestar/go-commons-pool/v2"
)

// cluster_db用于对key的路由
type clusterDatabase struct {
	self           string
	nodes          []string                    // 所有节点
	peerPicker     *consistenthash.NodeMap     // 节点的添加和选择
	peerConnection map[string]*pool.ObjectPool // Map<node, 连接池>
	db             databaseface.Database       // 单机database
}

func MakeClusterDatabase() *clusterDatabase {
	cluster := &clusterDatabase{
		self:           config.Properties.Self,
		db:             database.NewDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}

	// 记录所有节点
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)

	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}

	cluster.nodes = nodes
	return cluster
}

// 表示Redis的指令类型
type CmdFunc func(cluster *clusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

var router = makeRouter()

// clusterDatabase执行命令逻辑
func (cluster *clusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}

	result = cmdFunc(cluster, c, cmdLine)
	return
}

func (cluster *clusterDatabase) Close() {
	cluster.db.Close()
}

func (cluster *clusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
