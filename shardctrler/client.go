package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	requestId int
	leaderId  int
	clientId  int64
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
	// Your code here.

	ck.clientId = nrand()
	ck.leaderId = int(nrand()) % len(servers)
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.requestId++
	args.Num = num
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	serverId := ck.leaderId
	for {
		// try each known server.

		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Query", args, &reply)
		if ok {
			if reply.WrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
				continue
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Config
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.requestId++
	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	serverId := ck.leaderId
	for {
		// try each known server.

		reply := JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
				continue
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.requestId++
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestId = ck.requestId
	serverId := ck.leaderId

	for {
		// try each known server.

		reply := LeaveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
				continue
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.requestId++
	args.Shard = shard
	args.GID = gid
	args.RequestId = ck.requestId
	serverId := ck.leaderId

	for {
		// try each known server.

		reply := MoveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
				continue
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
