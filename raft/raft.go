package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

//----------------------------------------------自定义常量和枚举类型---------------------------------------------------------\

//是否打印log到控制台
var ifLog = false

type RaftState int //raft节点的状态

const (
	Follower RaftState = iota
	Candidate
	Leader
)

//全局心跳超时时间（75ms）和超时等待时间（100-200ms）
const (
	HeartsBeatsTimeout = 10
	AppliedSleep       = 5
	MinVoteTime        = 75
	MoreVoteTime       = 100
)

//-----------------------------------------------实体结构体对象------------------------------------------------------------\

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Log  interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state RaftState

	//持久变量
	currentTerm int //当前任期
	logEntries  []LogEntry
	votedFor    int //当前任期内获得选票的候选人ID,-1表示没有

	//易变的变量
	commitIndex int //最大提交的条目索引
	lastApplied int //最后被写到状态机的条目索引
	applyChan   chan ApplyMsg
	voteNum     int       //竞选者精选leader时获得的票数
	voteTimer   time.Time //投票的时间，每次收到心跳信息或发起投票后重置

	//leader中易变的变量
	nextIndex  []int //每个peer的下一条要发送的日志条目索引
	matchIndex []int //每个peer已复制的日志条目索引

	//snapshot需要的变量
	lastIncludedTerm  int //lastIncludeTerm的任期
	lastIncludedIndex int //install快照后从该索引位置提交日志
}

//----------------------------------------------------RPC消息结构体------------------------------------------------------------\

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期
	CandidateId  int //候选人ID
	LastLogIndex int //候选人最后一个日志条目的索引
	LastLogTerm  int //候选人最后一个日志条目的任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前的任期，用于给候选人更新
	VoteGranted bool //是否获得选票
}

type AppendEntriesArgs struct {
	//(2A 2B)
	Term         int        //领导人的任期
	LeaderId     int        //领导人的ID
	PrevLogIndex int        //最新日志前一条日志的索引
	PrevLogTerm  int        //前一条日志的任期
	Entries      []LogEntry //存储的日志条目
	LeaderCommit int        //领导人已提交的日志索引
}

type AppendEntriesReply struct {
	//(2A 2B)
	Term        int  //当前任期,供leader更新
	Success     bool //是否满足同步条件
	UpNextIndex int  //如果日志冲突，返回给leader下次要更新的index
}

type InstallSnapshotArgs struct {
	Term              int    //leader的term
	LeaderId          int    //为了让追随者可以对客户请求重定向
	LastIncludedIndex int    //快照替换所有条目，包括该索引
	LastIncludedTerm  int    //lastIncludedIndex的任期
	Data              []byte //快照数据
}

type InstallSnapshotReply struct {
	Term int
}

//------------------------------------------------------test调用的外部接口--------------------------------------------------------------\

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果index大于自身的commitIndex，则不能安装此快照,如果index不大于lastIncludeIndex，则不必安装此快照
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}

	//更新快照日志,截断index以后的日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	if ifLog {
		log.Printf("[Snapshot-Rf(%v)]: rf.commitIndex:%v,index:%v\n", rf.me, rf.commitIndex, index)
	}
	//更新快照信息,如果index比log日志条目数要多的话，lastIncludeTerm为lastTerm
	if index == rf.getLastIndex()+1 {
		rf.lastIncludedTerm = rf.getLastTerm()
	} else {
		rf.lastIncludedTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludedIndex = index
	rf.logEntries = sLogs

	//重置commitIndex，lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	//持久化快照
	state := rf.getStateByte()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return index, term, false
	}

	isLeader = rf.state == Leader
	if !isLeader {
		return index, term, isLeader
	}

	index = rf.getLastIndex() + 1
	term = rf.currentTerm
	logEntry := LogEntry{
		Log:  command,
		Term: term,
	}

	rf.logEntries = append(rf.logEntries, logEntry)
	rf.persist()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.voteNum = 0
	rf.logEntries = []LogEntry{}
	//增加一条空日志，便于插入第一条日志
	rf.logEntries = append(rf.logEntries, LogEntry{})
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rand.Seed(time.Now().Unix() + int64(me))
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendTicker()
	go rf.committedTicker()

	return rf
}

//----------------------------------------------raft常用方法utils------------------------------------------------------------\

//生成随机的心跳过期时间
func generateOverTime(server int) int {
	rand.Seed(time.Now().Unix() + int64(server))
	return rand.Intn(MoreVoteTime) + MinVoteTime
}

// 验证竞选者是否比自己的日志新
// 最后一个日志条目的任期号大的更新，如果任期号相同，那么日志更长的新
func (rf *Raft) upToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

//获得节点日志条目的最后一个索引
func (rf *Raft) getLastIndex() int {
	return len(rf.logEntries) - 1 + rf.lastIncludedIndex
}

//获得LastIndex处的日志条目的Term
func (rf *Raft) getLastTerm() int {
	if len(rf.logEntries)-1 == 0 {
		//还未追加任何日志，返回快照的信息
		return rf.lastIncludedTerm
	} else {
		return rf.logEntries[len(rf.logEntries)-1].Term
	}
}

//通过快照信息获得真实的日志任期
func (rf *Raft) restoreLogTerm(index int) int {
	//如果当前index和快照信息的索引一致，那么返回快照的任期
	if index <= rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.logEntries[index-rf.lastIncludedIndex].Term
}

//通过快照信息获取真实的索引
func (rf *Raft) restoreLog(index int) LogEntry {
	return rf.logEntries[index-rf.lastIncludedIndex]
}

//获得某follower节点要追加的前一个日志的信息
func (rf *Raft) getProvLogInfo(server int) (index int, term int) {
	provLogIndex := rf.nextIndex[server] - 1

	//防止下标越界
	lastIndex := rf.getLastIndex()
	if provLogIndex == lastIndex+1 {
		provLogIndex = lastIndex
	}

	return provLogIndex, rf.restoreLogTerm(provLogIndex)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//-----------------------------------------------------tricker---------------------------------------------------------------\

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(rf.me)) * time.Millisecond)

		rf.mu.Lock()

		//时间过期发起选举
		//每次的voteTimer如果小于sleep之前的时间nowTime，则表示选举超时
		if rf.voteTimer.Before(nowTime) && rf.state != Leader {
			//转变为竞选者，发起投票
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()

			if ifLog {
				log.Printf("[++++elect++++] :Rf[%v] send a election\n", rf.me)
			}
			rf.sendElection()
			rf.voteTimer = time.Now()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(HeartsBeatsTimeout * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	for !rf.killed() {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Log,
			})
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyChan <- messages
			if ifLog {
				log.Printf("[++++commit++++]: rf[%v] committed log[%v] successfully\n", rf.me, messages.CommandIndex)
			}
		}
	}
}

//-------------------------------------------------election leader------------------------------------------------------------\

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		//开启协程对每个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastIndex(),
				LastLogTerm:  rf.getLastTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if ok {
				rf.mu.Lock()

				//判断自身是否还是竞选者，并且任期没有发生变化
				if rf.state != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				//收到的响应的任期大于args的任期（由于出现网络分区的情况）
				//此时应该变为跟随者，等待新leader的心跳或者发起新一轮选举
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.state = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				//响应结果同意，判断是否收到超过一半节点的选票
				if reply.VoteGranted && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {
						if ifLog {
							log.Printf("[++++elect++++] :Rf[%v] to be leader,term is : %v\n", rf.me, rf.currentTerm)
						}
						rf.state = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						//初始化rf的nextIndex和matchIndex
						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.voteTimer = time.Now()
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		if ifLog {
			log.Printf("[	RequestVote--Return-Rf(%v) 	] arg:%+v, reply:%+v\n", rf.me, args, reply)
		}
	}()

	if rf.killed() {
		//该节点crash
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//候选人的任期比跟随者的还小，说明该轮选票已经过期，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		//碰到任期更大的候选者了，说明自己已经落后了，重置自己的任期与当前任期的选票
		//但不一定选这个候选者，如果候选者的日志不如自己新，则拒绝投票，等待该任期的其他候选者的投票请求
		//此时不重置选票计时器，因为如果本次没选该候选者但重置了计时器，那么自己变成候选者的时机会延后，导致选出Leader的效率降低
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.voteNum = 0
		rf.votedFor = -1
		rf.persist()
	}

	// if ifLog {
	// 	log.Printf("[RequestVote--state-Rf(%v)]:rf.voteFor:%v,rf.voteNum:%v,rf.lastIndex:%v,rf.lastTerm:%v,rf.upToDate:%v\n",
	// 		rf.me, rf.votedFor, rf.voteNum, rf.getLastIndex(), rf.getLastTerm(), rf.upToDate(args.LastLogIndex, args.LastLogTerm))
	// }

	if !rf.upToDate(args.LastLogIndex, args.LastLogTerm) || //存在冲突
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { //该任期已经投过票并且没有投该Candidate
		//这两种情况下拒绝投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		//比自己新，且没投过票，投给他,并重置计时器
		rf.votedFor = args.CandidateId
		rf.voteTimer = time.Now()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		return
	}
}

//--------------------------------------------------append entries------------------------------------------------------------\

func (rf *Raft) leaderAppendEntries() {
	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		}

		//开启协程并发的发送日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			if rf.lastIncludedIndex > rf.nextIndex[server]-1 {
				//此时需要发送快照给server
				if ifLog {
					log.Printf("[     SendSnapShot-Raft[%v]   ] nextIndex[%v]:%v,lastIncludedIndex:%v", rf.me, server, rf.nextIndex[server], rf.lastIncludedIndex)
				}
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, preLogTerm := rf.getProvLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  preLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[server] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logEntries[rf.nextIndex[server]-rf.lastIncludedIndex:]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()

			ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				//响应的任期比该节点大，降为follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.voteTimer = time.Now()
					return
				}

				if reply.Success {
					rf.commitIndex = 0
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					//遍历下标是否满足,从lastindex遍历到快照index+1处，快照index处一定满足已提交
					for index := rf.getLastIndex(); index > rf.lastIncludedIndex; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						//如果大于一半，则修改commitIndex
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							break
						}
					}
				} else {
					//日志增量失败，有冲突
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
					}
				}
			}
		}(index)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		if ifLog && len(args.Entries) > 0 {
			log.Printf("[	AppendEntries--Return-Rf(%v) 	] arg:%+v, reply:%+v\n", rf.me, args, reply)
		}
	}()
	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		return
	}

	//出现网络分区时，可能导致leader的任期比follower的小
	//此时应拒绝追加日志，并返回给老leader最新的任期
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.UpNextIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.voteTimer = time.Now()

	//检查是否有冲突,前提这不是第一条日志
	//1. 如果preLogIndex的大于当前日志的最大的下标说明Follower日志缺失，拒绝附加日志
	//2. 如果preLogIndex处的任期和preLogTerm不相等，说明存在冲突，拒绝附加日志
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex()
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex - 1; index >= 0; index-- {
				if rf.restoreLogTerm(index) != tempTerm {
					reply.UpNextIndex = index + 1
					break
				}
			}
			return
		}
	}

	//进行到此说明args.PrevLogIndex之前的日志完全同步
	//截取日志
	if args.PrevLogIndex+1-rf.lastIncludedIndex >= 0 {
		rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
		rf.persist()

		//commitIndex取leaderCommit和last new entry最小值的原因是：虽然应该更新到leaderCommit，但是new entry的下标更小
		//说明日志不存在，更新commit的目的是为了applied log，这样会导致日志下标溢出。
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit <= rf.getLastIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastIndex()
			}
		}
	}
}

//----------------------------------------------------Persist---------------------------------------------------------------\

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) getStateByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码解码时变量名必须一致
	currentTerm := rf.currentTerm
	voteFor := rf.votedFor
	logEntries := rf.logEntries
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	e.Encode(currentTerm)
	e.Encode(voteFor)
	e.Encode(logEntries)
	e.Encode(lastIncludedIndex)
	e.Encode(lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).

	data := rf.getStateByte()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		if ifLog {
			log.Printf("decode error\n")
		}
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		if ifLog {
			log.Printf("RaftNode[%d] persist read, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, currentTerm, votedFor, logEntries)
		}
	}
}

//-----------------------------------------------------Snapshot-----------------------------------------------------------------\

func (rf *Raft) leaderSendSnapShot(server int) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapShot", &args, &reply)
	if ok {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		//如果返回的term比自身大，变成follower
		if rf.currentTerm < reply.Term {
			rf.votedFor = -1
			rf.voteNum = 0
			rf.state = Follower
			rf.persist()
			rf.voteTimer = time.Now()
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer func() {
		if ifLog {
			log.Printf("[	InstallSnapShot--Return-Rf(%v) 	] arg:{Term:%v,LeaderId:%v,LastIncludedIndex:%v,LastIncludedTerm:%v}, reply:%v\n", rf.me, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, reply)
		}
	}()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	//退回状态为追随者
	rf.state = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.voteTimer = time.Now()

	if rf.lastIncludedIndex > args.LastIncludedIndex {
		//没必要安装该快照
		rf.mu.Unlock()
		return
	}

	//保存快照文件，丢弃任何现有的或具有较小索引的部分快照
	index := args.LastIncludedIndex
	tempLog := make([]LogEntry, 0)
	tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	//使用快照内容重置状态机
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.logEntries = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.getStateByte(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyChan <- msg
}
