package cluster

import (
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/resp/reply"
)

func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = ping
	routerMap["del"] = Del
	routerMap["exists"] = defaultFunc
	routerMap["type"] = defaultFunc
	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["flushdb"] = FlushDB
	routerMap["select"] = execSelect
	return routerMap
}

func defaultFunc(cluster *clusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	key := string(args[1])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, args)
}

func ping(cluster *clusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}

// 修改key的name，两个key的hash必须在同一个节点中
func Rename(cluster *clusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	} else {
		return cluster.relay(srcPeer, c, args)
	}
}

func FlushDB(cluster *clusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &reply.OkReply{}
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}

// del指令实现
func Del(cluster *clusterDatabase, c resp.Connection, args [][]byte) resp.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}

// select指令实现
func execSelect(cluster *clusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
