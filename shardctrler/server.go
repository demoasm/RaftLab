package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type OpTypes int

const (
	JoinType OpTypes = iota
	LeaveType
	MoveType
	QueryType
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	waitChanMap map[int]chan Op
	requestMap  map[int64]int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpType    OpTypes
	RequestId int
	ClientId  int64

	//Join
	Servers map[int][]string
	//Leave
	GIDs []int
	//Move
	Shard int
	GID   int
	//Quary
	Num    int
	Config Config
}

//借助优先队列实现负载均衡
type groupItem struct {
	shardNum int
	gid      int
}

type groupQueue []*groupItem

//实现sort接口
func (gq groupQueue) Len() int { return len(gq) }

func (gq groupQueue) Less(i, j int) bool {
	//小顶堆
	if gq[i].shardNum != gq[j].shardNum {
		return gq[i].shardNum > gq[j].shardNum
	} else {
		return gq[i].gid < gq[j].gid
	}
}

func (gq groupQueue) Swap(i, j int) {
	gq[i], gq[j] = gq[j], gq[i]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	defer func() {
		sc.mu.Lock()
		DPrintf("[	Join-Server[%v]	] args: %v, reply: %v, sc.lastconfig%v\n", sc.me, args, reply, sc.configs[len(sc.configs)-1])
		sc.mu.Unlock()
	}()

	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OpType:    JoinType,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		Servers:   args.Servers,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getwaitChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, index)
		sc.mu.Unlock()
	}()
	//设置定时器
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId == args.ClientId && replyOp.RequestId == args.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
			return
		} else {
			reply.WrongLeader = true
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	defer func() {
		sc.mu.Lock()
		DPrintf("[	Leave-Server[%v]	] args: %v, reply: %v, sc.lastconfig%v\n", sc.me, args, reply, sc.configs[len(sc.configs)-1])
		sc.mu.Unlock()
	}()
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OpType:    LeaveType,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		GIDs:      args.GIDs,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getwaitChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, index)
		sc.mu.Unlock()
	}()
	//设置定时器
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId == args.ClientId && replyOp.RequestId == args.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
			return
		} else {
			reply.WrongLeader = true
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	defer func() {
		sc.mu.Lock()
		DPrintf("[	Move-Server[%v]	] args: %v, reply: %v, sc.lastconfig%v\n", sc.me, args, reply, sc.configs[len(sc.configs)-1])
		sc.mu.Unlock()
	}()
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OpType:    MoveType,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		Shard:     args.Shard,
		GID:       args.GID,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getwaitChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, index)
		sc.mu.Unlock()
	}()
	//设置定时器
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId == args.ClientId && replyOp.RequestId == args.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
			return
		} else {
			reply.WrongLeader = true
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	defer func() {
		sc.mu.Lock()
		DPrintf("[	Query-Server[%v]	] args: %v, reply: %v, sc.lastconfig%v\n", sc.me, args, reply, sc.configs[len(sc.configs)-1])
		sc.mu.Unlock()
	}()
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{
		OpType:    QueryType,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
		Num:       args.Num,
	}
	index, _, _ := sc.rf.Start(op)
	ch := sc.getwaitChan(index)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChanMap, index)
		sc.mu.Unlock()
	}()
	//设置定时器
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId == args.ClientId && replyOp.RequestId == args.RequestId {
			reply.Err = OK
			reply.WrongLeader = false
			reply.Config = replyOp.Config
			return
		} else {
			reply.WrongLeader = true
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		return
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChanMap = make(map[int]chan Op)
	sc.requestMap = make(map[int64]int)

	go sc.applyMsgHandler()

	return sc
}

func (sc *ShardCtrler) getwaitChan(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.waitChanMap[index]
	if !ok {
		sc.waitChanMap[index] = make(chan Op, 1)
		return sc.waitChanMap[index]
	}
	return ch
}

func (sc *ShardCtrler) checkLinear(clientId int64, index int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastIndex, ok := sc.requestMap[clientId]
	if !ok {
		return true
	}
	return index > lastIndex
}

func (sc *ShardCtrler) applyMsgHandler() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			if sc.checkLinear(op.ClientId, op.RequestId) {
				//处理该操作
				sc.mu.Lock()
				switch op.OpType {
				case JoinType:
					sc.joinHandler(op.Servers)
				case LeaveType:
					sc.leaveHandler(op.GIDs)
				case MoveType:
					sc.moveHandler(op.Shard, op.GID)
				case QueryType:
					op.Config = sc.queryHandler(op.Num)
				}
				sc.requestMap[op.ClientId] = op.RequestId
				sc.mu.Unlock()
			}
			sc.getwaitChan(index) <- op
		}
	}
}

func (sc *ShardCtrler) joinHandler(Servers map[int][]string) {
	oldconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{Num: len(sc.configs)}
	newServer := make(map[int][]string)
	//将旧server加入新servers
	for gid, servers := range oldconfig.Groups {
		newServer[gid] = make([]string, len(servers))
		copy(newServer[gid], servers)
	}
	//将新server加入新servers
	for gid, servers := range Servers {
		_, ok := oldconfig.Groups[gid]
		if !ok {
			newServer[gid] = servers
		}
	}
	newconfig.Groups = newServer
	newconfig.Shards = oldconfig.Shards
	//统计当前分片在新servers的分配情况，构建groupQueue
	groupMap := make(map[int]int) //GID -> shardNum
	for _, gID := range oldconfig.Shards {
		if gID != 0 {
			groupMap[gID]++
		}
	}

	if len(newServer) == 0 {
		sc.configs = append(sc.configs, Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newServer,
		})
		return
	}

	newconfig.Shards = sc.loadBalance(newServer, groupMap, newconfig.Shards)

	sc.configs = append(sc.configs, newconfig)
}

func (sc *ShardCtrler) leaveHandler(GIDs []int) {
	oldconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{Num: len(sc.configs)}
	newServer := make(map[int][]string)
	leaveMap := make(map[int]bool) //Gid -> 是否包含在GIDs中
	//深拷贝newServer
	for gid, servers := range oldconfig.Groups {
		newServer[gid] = servers
		leaveMap[gid] = false
	}
	//删除掉newServer中gid在GIDs中的group
	for _, gid := range GIDs {
		delete(newServer, gid)
		leaveMap[gid] = true
	}
	newconfig.Groups = newServer

	groupMap := make(map[int]int) //GID -> shardNum
	newconfig.Shards = oldconfig.Shards
	for sid, gid := range newconfig.Shards {
		if leaveMap[gid] {
			newconfig.Shards[sid] = 0
		}
	}
	for _, gid := range newconfig.Shards {
		if gid != 0 {
			groupMap[gid]++
		}
	}

	if len(newServer) == 0 {
		sc.configs = append(sc.configs, Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newServer,
		})
		return
	}

	//负载均衡
	newconfig.Shards = sc.loadBalance(newServer, groupMap, newconfig.Shards)
	sc.configs = append(sc.configs, newconfig)
}

//将指定分片Shard分配给指定group GID
func (sc *ShardCtrler) moveHandler(Shard int, GID int) {
	oldconfig := sc.configs[len(sc.configs)-1]
	newconfig := Config{Num: len(sc.configs)}
	newServer := make(map[int][]string)
	//深拷贝newServer
	for gid, servers := range oldconfig.Groups {
		newServer[gid] = servers
	}
	newconfig.Groups = newServer
	newconfig.Shards = oldconfig.Shards
	newconfig.Shards[Shard] = GID
	sc.configs = append(sc.configs, newconfig)
}

func (sc *ShardCtrler) queryHandler(Num int) Config {
	if Num == -1 || Num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[Num]
}

func (sc *ShardCtrler) loadBalance(newServer map[int][]string, GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	var gq groupQueue
	for gid, _ := range newServer {
		gq = append(gq, &groupItem{
			shardNum: GroupMap[gid],
			gid:      gid,
		})
	}
	//对gq进行排序,第一优先级为shardNum(大到小) 第二优先级为gid(小到大)
	sort.Sort(gq)
	//
	avg := NShards / len(newServer)
	remainder := NShards % len(newServer)
	//第一遍遍历gq,找出过载的group
	for index, groupitem := range gq {
		target := avg
		if index < remainder {
			target++
		}
		if groupitem.shardNum > target {
			//过载
			freeNum := groupitem.shardNum - target
			for index, gid := range lastShards {
				if freeNum <= 0 {
					break
				}
				if gid == groupitem.gid {
					lastShards[index] = 0
					freeNum--
				}
			}
		}
	}
	//第二遍遍历gq,找到负载不足的group
	for index, groupitem := range gq {
		target := avg
		if index < remainder {
			target++
		}
		if groupitem.shardNum < target {
			//负载不足
			appendNum := target - groupitem.shardNum

			for index, gid := range lastShards {
				if appendNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[index] = groupitem.gid
					appendNum--
				}
			}
		}
	}

	return lastShards
}
