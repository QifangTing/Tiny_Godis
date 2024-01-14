package database

import (
	"Tiny_Godis/config"
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/lib/logger"
	"Tiny_Godis/resp/reply"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
)

// Database：一组db的集合
type Database struct {
	dbSet []*DB
}

func NewDatabase() *Database {
	mdb := &Database{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16 // 默认16，读配置
	}

	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	return mdb
}

// 实现interface/database
func (mdb *Database) Close() {
}

func (mdb *Database) AfterClientClose(c resp.Connection) {
}

// Exec：执行切换db指令或者其他指令
func (mdb *Database) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	} else {
		dbIndex := c.GetDBIndex()
		selectedDB := mdb.dbSet[dbIndex]
		return selectedDB.Exec(c, cmdLine)
	}
}

// execSelect方法：选择db（指令：select 2）
func execSelect(c resp.Connection, mdb *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
