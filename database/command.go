package database

import "strings"

// 记录所有指令和command结构体的关系
var cmdTable = make(map[string]*command)

// 每一个command结构体都是一个指令，例如ping，keys等
type command struct {
	executor ExecFunc
	arity    int // 参数数量
}

// 注册指令的方法
func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name) // 统一小写
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
