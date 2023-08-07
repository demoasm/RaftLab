package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId  int64 //客户端id
	RequestId int   //自增的请求id
	LeaderId  int   //leader的id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.RequestId = 0
	ck.LeaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.RequestId++
	args := GetArgs{
		Key:       key,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}
	serverId := ck.LeaderId
	for {
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.LeaderId = serverId
				// DPrintf("clinet[%v] Get(key:%v), return: %v", ck.ClientId, key, reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.LeaderId = serverId
				// DPrintf("clinet[%v] Get(key:%v), return: %v", ck.ClientId, key, "")
				return ""
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		//请求失败，s换下一个server请求
		serverId = (serverId + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// defer DPrintf("clinet[%v] PutAppend(key:%v,value:%v,op:%v)", ck.ClientId, key, value, op)
	ck.RequestId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}
	serverId := ck.LeaderId
	for {
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.LeaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		//请求失败，s换下一个server请求
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
