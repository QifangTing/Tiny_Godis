package resp

// 各种服务端对客户端的回复
type Reply interface {
	ToBytes() []byte // 将回复的内容转为字节
}
