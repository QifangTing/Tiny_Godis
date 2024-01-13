package database

import (
	"Tiny_Godis/interface/resp"
	"Tiny_Godis/lib/wildcard"
	"Tiny_Godis/resp/reply"
)

// execDel：del k1 k2 k3 ...
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

// execExists：exist k1 k2 k3 ...
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// execFlushDB：flushdb
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	return &reply.OkReply{}
}

// execType：type k1
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}

	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}
	return &reply.UnknownErrReply{}
}

// execRename：rename k1 k2
func execRename(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return &reply.OkReply{}
}

// esecRenameNx: renamenx k1 k2
func execRenameNx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	// dest 已经存在，就直接返回
	_, exist := db.GetEntity(dest)
	if exist {
		return reply.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return reply.MakeIntReply(1)
}

// execKeys：keys *
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)

	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("Del", execDel, -2)
	RegisterCommand("Exist", execExists, -2)
	RegisterCommand("FlushDB", execFlushDB, -1)
	RegisterCommand("Rename", execRename, 3)
	RegisterCommand("RenameNx", execRenameNx, 3)
	RegisterCommand("Keys", execKeys, 2)
}
