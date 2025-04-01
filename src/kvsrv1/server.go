package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value_and_Version struct {
	value   string
	version uint64
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv_store map[string]*Value_and_Version
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.

	// 初始化一个kv server。每个k-v是一个map，索引是key，值是value和version
	kv.kv_store = make(map[string]*Value_and_Version)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.

	// log.Printf("Get: key=%v\n", args.Key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	value_and_version := kv.kv_store[args.Key]

	// log.Printf("Query result: %v\n", value_and_version)
	if value_and_version == nil {
		reply.Err = rpc.ErrNoKey
		return
	}

	// reply.Value = key_and_version.value
	// reply.Version = rpc.Tversion(key_and_version.version)
	// reply.Err = rpc.OK

	// 替代写法
	*reply = rpc.GetReply{
		Value:   value_and_version.value,
		Version: rpc.Tversion(value_and_version.version),
		Err:     rpc.OK,
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// version为0表示插入新kv，否则表示更改，插入新kv时也要先看库中是否已存在该key
	// log.Printf("Put: key=%s, value=%s, version=%d\n", args.Key, args.Value, args.Version)

	value_and_version := kv.kv_store[args.Key]
	// log.Printf("Query result: key: %s, %+v\n", args.Key, value_and_version)

	if value_and_version == nil { // 库中没有检索到该key
		if args.Version == 0 { // 是插入新key的情况，找不到很正常
			kv.kv_store[args.Key] = &Value_and_Version{args.Value, 1}
			reply.Err = rpc.OK
		} else { // 不是插入新key，纯粹是找不到
			reply.Err = rpc.ErrNoKey
		}
	} else { // 检索到该key了，那么看version
		if value_and_version.version == uint64(args.Version) { // key和version都匹配，更新
			value_and_version.value = args.Value
			value_and_version.version++
			reply.Err = rpc.OK
		} else { // version不匹配
			reply.Err = rpc.ErrVersion

		}
	}

	// 当前store中内容
	// log.Println("---------------------")
	// log.Println("kv_store cotent currently:")
	// for key, entry := range kv.kv_store {
	// 	log.Printf("%s: %+v\n", key, entry)
	// }
	// log.Println()
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
