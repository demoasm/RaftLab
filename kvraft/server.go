package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	PutOp OpType = iota
	AppendOp
	GetOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  OpType //"Put" or "Append" or "Get"

	//client
	ClientId  int64
	RequestId int

	//raft
	Index int //raft返回的日志索引号
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// requestMap  map[int64]int     //存储每个client的请求序列号
	// kvDatebase  map[string]string //用于持久化存储已提交的命令
	// waitChanMap map[int]chan Op   //用于等待命令被提交

	RequestMap  map[int64]int     //存储每个client的请求序列号
	KvDatebase  map[string]string //用于持久化存储已提交的命令
	waitChanMap map[int]chan Op   //用于等待命令被提交

	lastincludeIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// defer DPrintf("[-----GET----]: KVServerId:%v args:%v, reply:%v", kv.me, args, reply)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Type:      GetOp,
	}
	index, _, _ := kv.rf.Start(op)
	ch := kv.getWaitChan(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChanMap, index)
		kv.mu.Unlock()
	}()
	//设置超时时间
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case op2 := <-ch:
		if op.ClientId == op2.ClientId && op.RequestId == op2.RequestId {
			reply.Err = OK
			kv.mu.Lock()
			value, ok := kv.KvDatebase[op.Key]
			if !ok {
				reply.Err = ErrNoKey
			} else {
				reply.Value = value
			}
			kv.mu.Unlock()
			return
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
	defer timer.Stop()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// defer DPrintf("[-----PUTAPPEND----]: KVServerId:%v args:%v, reply:%v", kv.me, args, reply)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	switch args.Op {
	case "Put":
		op.Type = PutOp
	case "Append":
		op.Type = AppendOp
	default:
		reply.Err = ErrWrongLeader
		return
	}
	index, _, _ := kv.rf.Start(op)
	ch := kv.getWaitChan(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChanMap, index)
		kv.mu.Unlock()
	}()
	//设置超时时间
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case op2 := <-ch:
		if op.ClientId == op2.ClientId && op.RequestId == op2.RequestId {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrWrongLeader
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
	defer timer.Stop()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KvDatebase = make(map[string]string)
	kv.RequestMap = make(map[int64]int)
	kv.waitChanMap = make(map[int]chan Op)

	kv.lastincludeIndex = -1

	//如果crash，重启后从快照中恢复数据
	snapshot := persister.ReadSnapshot()

	if len(snapshot) > 0 {
		kv.installSnapShot(snapshot)
	}

	go kv.applyMsgHandler()

	return kv
}

//---------------------------------------------循环处理提交的指令-----------------------------------------------------------

func (kv *KVServer) applyMsgHandler() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				index := msg.CommandIndex

				if index <= kv.lastincludeIndex {
					return
				}

				op := msg.Command.(Op)
				if kv.checkLinear(op.ClientId, op.RequestId) {
					kv.mu.Lock()
					switch op.Type {
					case PutOp:
						kv.KvDatebase[op.Key] = op.Value
					case AppendOp:
						value, ok := kv.KvDatebase[op.Key]
						if ok {
							buffer := bytes.NewBufferString(value)
							buffer.WriteString(op.Value)
							kv.KvDatebase[op.Key] = buffer.String()
						} else {
							kv.KvDatebase[op.Key] = op.Value
						}
					}
					kv.RequestMap[op.ClientId] = op.RequestId
					kv.mu.Unlock()
					if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
						//需要快照
						//将kvDatabase、requestMap持久化存储
						kv.snapshot(msg.CommandIndex)
					}
				}
				kv.getWaitChan(index) <- op
			} else if msg.SnapshotValid {
				//安装快照
				kv.mu.Lock()
				kv.installSnapShot(msg.Snapshot)
				kv.lastincludeIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) checkLinear(clientId int64, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastrequestId, ok := kv.RequestMap[clientId]
	if !ok {
		return true
	}
	return requestId > lastrequestId
}

func (kv *KVServer) getWaitChan(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitChanMap[index]
	if !ok {
		//需要开辟一个缓冲区，防止阻塞
		kv.waitChanMap[index] = make(chan Op, 1)
		return kv.waitChanMap[index]
	}
	return ch
}

func (kv *KVServer) snapshot(index int) {
	//编码
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	KvDataBase := kv.KvDatebase
	RequestMap := kv.RequestMap

	e.Encode(KvDataBase)
	e.Encode(RequestMap)

	data := w.Bytes()

	kv.mu.Unlock()
	DPrintf("[------SnapShot-------]: server[%v] start snapshot, index: %v", kv.me, index)
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) installSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var KvDataBase map[string]string
	var RequestMap map[int64]int

	if d.Decode(&KvDataBase) != nil ||
		d.Decode(&RequestMap) != nil {
		DPrintf("decode error!!!")
	}

	DPrintf("[------SnapShot-------]: server[%v] install snapshot, RequestMap: %v", kv.me, RequestMap)
	kv.KvDatebase = KvDataBase
	kv.RequestMap = RequestMap
}
