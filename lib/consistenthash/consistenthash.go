package consistenthash

import (
	"hash/crc32"
	"sort"
)

// hash函数定义（Go的 hash函数就是这样定义的
type HashFunc func(data []byte) uint32

// 存储所有节点和节点的hash
type NodeMap struct {
	hashFunc    HashFunc
	nodeHashs   []int          // 各节点的hash值，顺序存放
	nodehashMap map[int]string // <hash, 节点>
}

func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

// 添加节点到一致性哈希中
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" { // 跳过空
			continue
		}
		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodehashMap[hash] = key
	}
	sort.Ints(m.nodeHashs)
}

// 选择节点。使用二分查找，注意取模
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hashFunc([]byte(key)))

	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})
	if idx == len(m.nodeHashs) { // 到第一个点
		idx = 0
	}

	return m.nodehashMap[m.nodeHashs[idx]]
}
