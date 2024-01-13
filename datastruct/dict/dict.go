package dict

// 遍历字典所有的键值对，返回值是布尔，true继续遍历，false停止遍历
type Consumer func(key string, val interface{}) bool

/*
Redis数据结构的接口，使用 sync.Map 作为字典的实现
（如果想用别的数据结构，换一个实现即可
*/
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
