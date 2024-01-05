package resp

// 同客户端的连接
type Connection interface {
	Write([]byte) error
	GetDBIndex() int // 区分当前不同的DB
	SelectDB(int)
}
