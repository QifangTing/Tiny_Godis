package aof

import (
	"Tiny_Godis/config"
	databaseface "Tiny_Godis/interface/database"
	"Tiny_Godis/lib/logger"
	"Tiny_Godis/lib/utils"
	"Tiny_Godis/resp/connection"
	"Tiny_Godis/resp/parser"
	"Tiny_Godis/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AofHandler：1.从管道中接收数据 2.写入AOF文件
type AofHandler struct {
	db          databaseface.Database
	aofChan     chan *payload // 写文件的缓冲区
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

// NewAofHandler
func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db

	// 加载AOF
	handler.LoadAof()

	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile

	handler.aofChan = make(chan *payload, 1<<16)
	go func() {
		handler.handleAof() // 协程调用，写入磁盘
	}()

	return handler, err
}

// AddAof：用户的指令包装成payload放入管道
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{ // 打开追加、aofChan完成初始化
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof：将管道中的payload写入磁盘
func (handler *AofHandler) handleAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB { // 插入select语句
			tmp := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
			data := reply.MakeMultiBulkReply(tmp).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			handler.currentDB = p.dbIndex
		} else {
			data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
			}
		}
	}
}

// LoadAof：重启Redis后加载aof文件
func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Warn(err)
		return
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{} // 用来记录DB

	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply) // 类型断言
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}
