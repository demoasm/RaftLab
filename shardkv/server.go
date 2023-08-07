package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key           string
	Value         string
	Type          string //"Put" or "Append" or "Get"
	Config        shardctrler.Config
	ShardId       int
	Shard         Shard
	LastRequestId map[int64]int
	LastConfigNum int
	ServerName    string

	//Valid
	ConfigValid   bool
	KeyValueValid bool
	//client
	ClientId  int64
	RequestId int

	//raft
	Index int //raft返回的日志索引号
}

type OpReply struct {
	ClientId  int64
	RequestId int
	Err       Err
}

type Shard struct {
	KvMap     map[string]string
	ConfigNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	dead int32 // set by Kill()
	// Your definitions here.
	sck        *shardctrler.Clerk
	config     shardctrler.Config
	lastconfig shardctrler.Config

	RequestMap         map[int64]int              //存储每个client的请求序列号
	ShardDatabase      [shardctrler.NShards]Shard //用于持久化存储已提交的命令
	LastConfigDatabase [shardctrler.NShards]Shard //存储上个配置被更新时的最新状态
	waitChanMap        map[int]chan OpReply       //用于等待命令被提交
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer DPrintf("[		Get-Server[%v]-Group[%v]		] args:%v, reply:%v\n", kv.me, kv.gid, args, reply)
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	gid := kv.config.Shards[shardId]
	if gid != kv.gid {
		//错误的组
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:           args.Key,
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		Type:          "Get",
		KeyValueValid: true,
		ConfigValid:   false,
	}
	err := kv.startOp(op)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.ShardDatabase[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else if _, ok := kv.ShardDatabase[shardId].KvMap[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = kv.ShardDatabase[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer DPrintf("[		PutAppend-Server[%v]-Gropu[%v]		] args:%v, reply:%v\n", kv.me, kv.gid, args, reply)
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	gid := kv.config.Shards[shardId]
	if gid != kv.gid {
		//错误的组
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:           args.Key,
		Value:         args.Value,
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		Type:          args.Op,
		KeyValueValid: true,
		ConfigValid:   false,
	}
	reply.Err = kv.startOp(op)
}

func (kv *ShardKV) AddShard(args *AddShardArg, reply *AddShardReply) {
	defer DPrintf("[		AddShard-Server[%v]-Group[%v]		] args:%v, reply:%v\n", kv.me, kv.gid, args, reply)
	op := Op{
		Type:          "AddShard",
		KeyValueValid: false,
		ConfigValid:   true,
		ClientId:      args.ClientId,
		RequestId:     args.RequestId,
		ShardId:       args.ShardId,
		Shard:         args.Shard,
		LastRequestId: args.LastRequestId,
	}
	reply.Err = kv.startOp(op)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.sck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//kv.config = kv.sck.Query(-1)
	kv.RequestMap = make(map[int64]int)
	kv.waitChanMap = make(map[int]chan OpReply)

	//如果crash，重启后从快照中恢复数据
	snapshot := persister.ReadSnapshot()

	if len(snapshot) > 0 {
		kv.installSnapShot(snapshot)
	}
	//如果应有的切片没有从快照中读取到，那么则读取旧配置，从旧配置中获取该切片的最新状态
	// go kv.checkNotArrivedShard()

	go kv.applyMsgListener()
	go kv.upConfigListener()
	return kv
}

func (kv *ShardKV) getWaitChan(index int) chan OpReply {
	ch, ok := kv.waitChanMap[index]
	if !ok {
		//需要开辟一个缓冲区，防止阻塞
		kv.waitChanMap[index] = make(chan OpReply, 1)
		return kv.waitChanMap[index]
	}
	return ch
}

func (kv *ShardKV) checkLinear(clientId int64, requestId int) bool {
	lastrequestId, ok := kv.RequestMap[clientId]
	if !ok {
		return true
	}
	return requestId > lastrequestId
}

func (kv *ShardKV) upConfigListener() {
	for !kv.killed() {
		if _, ifLeader := kv.rf.GetState(); !ifLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		curConfig := kv.config
		//对比新旧配置，查看是否还有未推送的切片
		if !kv.sendAll() {
			//存在未推送的切片，将未推送的切片全部发出
			for shardId, gid := range curConfig.Shards {
				if kv.lastconfig.Shards[shardId] == kv.gid && gid != kv.gid && kv.ShardDatabase[shardId].ConfigNum < curConfig.Num {
					//未推送的切片，将其推送出去
					args := AddShardArg{
						ShardId:   shardId,
						ClientId:  int64(kv.me),
						RequestId: curConfig.Num,
					}
					//深拷贝RequestMap和ShardMap
					args.Shard.ConfigNum = curConfig.Num
					args.Shard.KvMap = make(map[string]string)
					for k, v := range kv.ShardDatabase[shardId].KvMap {
						args.Shard.KvMap[k] = v
					}
					args.LastRequestId = make(map[int64]int)
					for clientId, requestId := range kv.RequestMap {
						args.LastRequestId[clientId] = requestId
					}

					//servers
					servers := make([]*labrpc.ClientEnd, 0)
					for _, name := range curConfig.Groups[gid] {
						servers = append(servers, kv.make_end(name))
					}
					//开启协程发送rpc请求
					go func(servers []*labrpc.ClientEnd, args *AddShardArg) {
						serverId := 0
						for {
							reply := AddShardReply{}
							ok := servers[serverId].Call("ShardKV.AddShard", args, &reply)
							if ok {
								kv.mu.Lock()
								// if _, ifLeader := kv.rf.GetState(); ifLeader {
								// 	DPrintf("[Server[%v]-Group[%v]] send shard[%v] in configNum[%v]\n", kv.me, kv.gid, args.ShardId, args.Shard.ConfigNum)
								// }
								if reply.Err == OK {
									//说明切片推送成功,要删除自身切片，并且开启了快照
									op := Op{
										Type:          "LeaveShard",
										ConfigValid:   true,
										KeyValueValid: false,
										ClientId:      int64(kv.me),
										RequestId:     curConfig.Num,
										ShardId:       args.ShardId,
									}
									kv.mu.Unlock()
									kv.startOp(op)
									break
								}
								kv.mu.Unlock()
							}
							serverId = (serverId + 1) % len(servers)
							if serverId == 0 {
								time.Sleep(100 * time.Millisecond)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		//对比新旧配置，查看是否还有未收到的切片
		if !kv.receivedAll() {
			//存在未收到的切片，sleep一段时间等待接受切片
			// sharids := []int{}
			// for shardId, gid := range kv.lastconfig.Shards {
			// 	if gid != kv.gid && kv.config.Shards[shardId] == kv.gid && kv.ShardDatabase[shardId].ConfigNum < kv.config.Num {
			// 		//旧配置里没分配给kv了但新配置里分配给了kv，并且该shard对应的配置号小于新配置的配置号
			// 		sharids = append(sharids, shardId)
			// 	}
			// }
			// DPrintf("Server[%v]-group[%v]waitting shards:%v in configNum: %v\n", kv.me, kv.gid, sharids, kv.config.Num)
			// DPrintf("Server[%v]-group[%v].ShardsDatabase: %v\n", kv.me, kv.gid, kv.ShardDatabase)
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		//走到这说明配置已经更新完成，轮询新配置
		sck := kv.sck
		kv.mu.Unlock()
		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			//未查询到新配置，进入下一个周期
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// DPrintf("\n--------------------------\nNewConfig:%v\nOldConfig:%v\n--------------------------\n", newConfig, curConfig)
		//走到这说明配置更新了
		//start一个更新配置的comment,利用raft达成组内的配置共识
		op := Op{
			ClientId:      int64(kv.me),
			RequestId:     newConfig.Num,
			ConfigValid:   true,
			KeyValueValid: false,
			Type:          "UpConfig",
			Config:        newConfig,
		}
		kv.startOp(op)
	}
}

func (kv *ShardKV) applyMsgListener() {
	for {
		if kv.killed() {
			return
		}
		for msg := range kv.applyCh {
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reOp := OpReply{
					ClientId:  op.ClientId,
					RequestId: op.RequestId,
					Err:       OK,
				}
				if op.KeyValueValid {
					shardId := key2shard(op.Key)
					if kv.config.Shards[shardId] != kv.gid {
						reOp.Err = ErrWrongGroup
					} else if kv.ShardDatabase[shardId].KvMap == nil {
						reOp.Err = ShardNotArrived
					} else if kv.checkLinear(reOp.ClientId, reOp.RequestId) {

						kv.RequestMap[op.ClientId] = op.RequestId
						switch op.Type {
						case "Put":
							kv.ShardDatabase[shardId].KvMap[op.Key] = op.Value
						case "Append":
							kv.ShardDatabase[shardId].KvMap[op.Key] += op.Value
						}
					}
				} else if op.ConfigValid {
					switch op.Type {
					case "UpConfig":
						//更新最新配置 UpConfigExecute(op Op)
						kv.upConfigExecute(op)
					case "AddShard":
						//增加切片 AddShardExecute(op Op)
						if kv.config.Num < op.Shard.ConfigNum {
							reOp.Err = ConfigNotArrived
						} else {
							//添加切片
							kv.addShardExecute(op)
						}
					case "LeaveShard":
						//删除切片
						kv.leaveShardExecute(op)
					}
				}
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					//需要快照
					//将kvDatabase、requestMap持久化存储
					kv.snapshot(msg.CommandIndex)
				}
				kv.getWaitChan(msg.CommandIndex) <- reOp
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				//安装快照
				kv.mu.Lock()
				kv.installSnapShot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) upConfigExecute(op Op) {
	oldConfig := kv.config
	newConfig := op.Config
	if oldConfig.Num >= newConfig.Num {
		//过时的配置，无需执行
		return
	}
	for shardId, gid := range newConfig.Shards {
		if gid == kv.gid && oldConfig.Shards[shardId] == 0 {
			//新配置分配到了，旧配置里还未分配，可以认为该切片还没有任何数据，新建map
			kv.ShardDatabase[shardId].KvMap = make(map[string]string)
			kv.ShardDatabase[shardId].ConfigNum = newConfig.Num
		}
	}
	kv.lastconfig = oldConfig
	kv.config = newConfig
}

func (kv *ShardKV) addShardExecute(op Op) {
	if kv.ShardDatabase[op.ShardId].KvMap != nil || kv.config.Num > op.Shard.ConfigNum {
		//说明该切片已经加入了，防止重复执行
		//或者该指令已过时，保证顺序执行
		return
	}

	//将op中的shard进行深拷贝
	kv.ShardDatabase[op.ShardId].KvMap = make(map[string]string)
	for key, value := range op.Shard.KvMap {
		kv.ShardDatabase[op.ShardId].KvMap[key] = value
	}
	kv.ShardDatabase[op.ShardId].ConfigNum = op.Shard.ConfigNum

	//更新kvServer的RequeatMap
	for clientId, requestId := range op.LastRequestId {
		if r, ok := kv.RequestMap[clientId]; !ok || r < requestId {
			//更新
			kv.RequestMap[clientId] = requestId
		}
	}
	// if _, ifleader := kv.rf.GetState(); ifleader {
	// 	DPrintf("[Server[%v]-Group[%v]] add shard[%v] in configNum[%v]\n", kv.me, kv.gid, op.ShardId, op.Shard.ConfigNum)
	// 	DPrintf("Server[%v]-group[%v].ShardsDatabase: %v\n", kv.me, kv.gid, kv.ShardDatabase)
	// }
}

func (kv *ShardKV) leaveShardExecute(op Op) {
	//kvServer推送成功的Shard需要删掉
	if op.RequestId < kv.config.Num {
		return
	}
	//GC掉时先拷贝到自己的lastConfigDatabase中,相当于删除的数据先缓存一个configNum，避免因为快照失败而导致数据丢失
	kv.LastConfigDatabase[op.ShardId].ConfigNum = kv.ShardDatabase[op.ShardId].ConfigNum
	kv.LastConfigDatabase[op.ShardId].KvMap = make(map[string]string)
	for key, value := range kv.ShardDatabase[op.ShardId].KvMap {
		kv.LastConfigDatabase[op.ShardId].KvMap[key] = value
	}

	kv.ShardDatabase[op.ShardId].KvMap = nil
	kv.ShardDatabase[op.ShardId].ConfigNum = op.RequestId
}

func (kv *ShardKV) snapshot(index int) {
	//编码
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	ShardDatabase := kv.ShardDatabase
	RequestMap := kv.RequestMap
	Config := kv.config
	LastConfig := kv.lastconfig
	e.Encode(ShardDatabase)
	e.Encode(RequestMap)
	e.Encode(Config)
	e.Encode(LastConfig)

	data := w.Bytes()

	DPrintf("[		SnapShot-Server[%v]-Group[%v]		] start snapshot, index: %v", kv.me, kv.gid, index)
	kv.rf.Snapshot(index, data)
}

func (kv *ShardKV) installSnapShot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ShardDatabase [shardctrler.NShards]Shard
	var RequestMap map[int64]int
	var Config shardctrler.Config
	var LastConfig shardctrler.Config

	if d.Decode(&ShardDatabase) != nil ||
		d.Decode(&RequestMap) != nil ||
		d.Decode(&Config) != nil ||
		d.Decode(&LastConfig) != nil {
		DPrintf("decode error!!!")
	}

	DPrintf("[		SnapShot-Server[%v]-Group[%v]		] install snapshot", kv.me, kv.gid)
	kv.ShardDatabase = ShardDatabase
	kv.RequestMap = RequestMap
	kv.config = Config
	kv.lastconfig = LastConfig
}

func (kv *ShardKV) startOp(op Op) (err Err) {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getWaitChan(index)
	kv.mu.Unlock()
	//设置超时时间
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case reOp := <-ch:
		kv.mu.Lock()
		delete(kv.waitChanMap, index)
		if op.ClientId == reOp.ClientId && op.RequestId == reOp.RequestId {
			err = reOp.Err
			kv.mu.Unlock()
			return
		} else {
			err = ErrInconsistentData
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		err = ErrOverTime
		return
	}
}

func (kv *ShardKV) sendAll() bool {
	//对比新旧配置，如果存在不一致且ShardDatabase对应的配置号比新配置的配置号更小，说明还有新切片未发送
	for shardId, gid := range kv.lastconfig.Shards {
		if gid == kv.gid && kv.config.Shards[shardId] != kv.gid && kv.ShardDatabase[shardId].ConfigNum < kv.config.Num {
			//旧配置里分配给kv了但新配置里没分配给kv，并且该shard对应的配置号小于新配置的配置号
			return false
		}
	}
	return true
}

func (kv *ShardKV) receivedAll() bool {
	//对比新旧配置，如果存在不一致并且ShardDatabase对应的配置号比新配置的配置号更小，说明还有新切片未收到
	for shardId, gid := range kv.lastconfig.Shards {
		if gid != kv.gid && kv.config.Shards[shardId] == kv.gid && kv.ShardDatabase[shardId].ConfigNum < kv.config.Num {
			//旧配置里没分配给kv了但新配置里分配给了kv，并且该shard对应的配置号小于新配置的配置号
			return false
		}
	}
	return true
}

//是否正在进行配置切换
// func (kv *ShardKV) transConfig() bool {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()
// 	return !(kv.sendAll() && kv.receivedAll())
// }
