package parser

import (
	"Tiny_Godis/interface/resp"
	"io"
)

type Payload struct {
	Data resp.Reply // 客服端给我们发的数据
	Err  error
}

type readState struct {
	readingMultiLine  bool     // 解析单行还是多行数据
	expectedArgsCount int      // 应该读取的参数个数
	msgType           byte     // 消息类型
	args              [][]byte // 消息内容
	bulkLen           int64    // 数据长度
}

// 判断解析是否完成
func (s *readState) finished() bool {
	// 需要解析的参数都被解析完
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// 异步解析数据后放入管道，返回管道数据
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 从管道中拿到数据，进行解析
func parse0(reader io.Reader, ch chan<- *Payload) {

}
