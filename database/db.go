package database

import (
	"Tiny_Godis/datastruct/dict"
	"Tiny_Godis/interface/database"
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/resp/reply"
	"strings"
)

// Redis中的分数据库
type DB struct {
	index  int
	data   dict.Dict
	addAof func(CmdLine)
}

// 所有Redis的指令都写成这样的类型
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// 二维的指令
type CmdLine = [][]byte

func makeDB() *DB {
	db := &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {},
	}
	return db
}

func (db *DB) Exec(c resp.Connection, cmdLine [][]byte) resp.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	// 校验参数个数
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor         // 拿到执行 set k v 中的set
	return fun(db, cmdLine[1:]) // 把 set k v 中的set切掉
}

// 定长：set k v => arity=3；
// 变长：exists k1 k2 k3 ... => arity=-2，表示参数>=2个
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
