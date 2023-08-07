package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	groupLeaders map[int]int //gid->leaderId

	requestId int
	// leadersId []int
	clientId int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.requestId = 0
	ck.clientId = nrand()
	//获取最新的配置
	ck.config = ck.sm.Query(-1)
	ck.groupLeaders = make(map[int]int)
	// DPrintf("%v\n", ck.config.Groups)
	for gid, serverlist := range ck.config.Groups {
		_, ok := ck.groupLeaders[gid]
		if !ok {
			ck.groupLeaders[gid] = int(nrand()) % len(serverlist)
		}
	}
	// DPrintf("%v\n", ck.config)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		serverId, ok := ck.groupLeaders[gid]
		for ok {
			srv := ck.make_end(ck.config.Groups[gid][serverId])
			var reply GetReply
			ok2 := srv.Call("ShardKV.Get", &args, &reply)
			if ok2 && (reply.Err == OK || reply.Err == ErrNoKey) {
				ck.groupLeaders[gid] = serverId
				return reply.Value
			}
			if ok2 && (reply.Err == ErrWrongGroup) {
				break
			}
			if ok2 && (reply.Err == ErrWrongLeader) || !ok2 {
				serverId = (serverId + 1) % len(ck.config.Groups[gid])
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		for gid, serverlist := range ck.config.Groups {
			_, ok := ck.groupLeaders[gid]
			if !ok {
				ck.groupLeaders[gid] = int(nrand()) % len(serverlist)
			}
		}
	}

	//return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: ck.requestId}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		serverId, ok := ck.groupLeaders[gid]
		for ok {
			srv := ck.make_end(ck.config.Groups[gid][serverId])
			var reply GetReply
			ok2 := srv.Call("ShardKV.PutAppend", &args, &reply)
			if ok2 && (reply.Err == OK || reply.Err == ErrNoKey) {
				ck.groupLeaders[gid] = serverId
				return
			}
			if ok2 && (reply.Err == ErrWrongGroup) {
				break
			}
			if ok2 && (reply.Err == ErrWrongLeader) || !ok2 {
				serverId = (serverId + 1) % len(ck.config.Groups[gid])
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		for gid, serverlist := range ck.config.Groups {
			_, ok := ck.groupLeaders[gid]
			if !ok {
				ck.groupLeaders[gid] = int(nrand()) % len(serverlist)
			}
		}
		// DPrintf("%v\n", ck.config)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
