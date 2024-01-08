package database

import "Tiny_Godis/interface/resp"

type CmdLine = [][]byte // 二维字节数组的指令别名

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply // 执行指令
	Close()
	AfterClientClose(c resp.Connection) // 关闭后
}

// 表示Redis的数据，包括string, list, set等等
type DataEntity struct {
	Data interface{}
}
