package database

import (
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// 启动程序时就会调用这个方法，随包初始化完成指令注册
func init() {
	RegisterCommand("ping", Ping, 1)
}
