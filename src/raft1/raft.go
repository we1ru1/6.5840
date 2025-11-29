package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"sort"

	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	LEADER    = "leader"
	CANDIDATE = "candidate"
	FOLLOWER  = "follower"
)

type LogEntry struct {
	Index   int // 直接添加索引，省略从数组位置推导
	Term    int
	Command interface{} // 这里将Command的类型由string改为interface{}，支持更多类型的命令
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // 服务器已知的最新任期号
	voteFor     int // 在当前任期中收到选票的候选人ID，如果没有则为-1
	logs        []LogEntry

	state string // 服务器的状态（leader，candidate，follower）

	commitIndex int // 已知被提交的最高日志条目的索引
	lastApplied int // 已应用于状态机的最高日志条目的索引

	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCh        chan raftapi.ApplyMsg
	applyCond      *sync.Cond   //  用于apply goroutine的condition variable
	replicatorCond []*sync.Cond // 用于replicator goroutine的condition variable

	lastIncludedIndex int    // 快照包含的最后一个日志条目的索引
	lastIncludedTerm  int    // 快照包含的最后一个日志条目的任期
	snapshot          []byte // 当前的快照数据x
}

func (rf *Raft) changeState(state string) {
	if rf.state == state {
		return
	}
	log.Printf("服务器 [%d] 状态 由 %v 变为 %v...\n", rf.me, rf.state, state)
	rf.state = state
	switch state {
	case FOLLOWER:
		rf.electionTimer.Reset(GenElectionTimeOut())
		rf.heartbeatTimer.Stop() // 停止心跳计时器
	case CANDIDATE:
	case LEADER:
		rf.electionTimer.Stop() // 停止选举计时器
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) encodeState() []byte {
	// 创建一个新的字节缓冲区
	w := new(bytes.Buffer)
	// 创建一个新的编码器
	e := labgob.NewEncoder(w)

	//编码需要持久化的状态
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)

	return w.Bytes()
}

// 将 Raft 的持久化状态保存到稳定的存储中，
// 以便在崩溃和重启后可以恢复。
// 有关哪些内容需要持久化的描述，请参见论文的图 2。
// 在你实现快照功能之前，应该将 nil 作为
// persister.Save() 的第二个参数传递。
// 在实现快照功能之后，传递当前的快照
// （如果没有快照则传递 nil）。
func (rf *Raft) persist() {
	// Your code here (3C).
	log.Printf("服务器[%d]已持久化状态：任期=%d, 投票=%d, 日志长度=%d",
		rf.me, rf.currentTerm, rf.voteFor, len(rf.logs))

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), nil)

}

// 恢复以前的持久状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		// 解码出错，记录错误但不终止程序
		log.Printf("服务器[%d]从持久化存储中恢复状态时发生错误", rf.me)
	}

	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs

	rf.lastApplied = rf.getFirstLog().Index
	rf.commitIndex = rf.getFirstLog().Index

	log.Printf("服务器[%d]从持久化存储中恢复状态：任期=%d, 投票=%d, 日志长度=%d",
		rf.me, rf.currentTerm, rf.voteFor, len(rf.logs))
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 服务端表示它已经创建了一个快照，其中包含
// 直到（并包括）指定index的所有信息。这意味着
// 服务端不再需要（包括）该索引及之前的日志。
// Raft 现在应该尽可能地修剪其日志。

// index 表示快照包含的最后一个日志条目的索引，snapshot是快照数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstLog().Index

	// 检查索引范围
	if index > rf.getLastLog().Index {
		log.Printf("服务器[%d] 错误：尝试为超出日志范围的索引创建快照，索引=%d，日志最大索引=%d",
			rf.me, index, rf.getLastLog().Index)
		return
	}

	// 如果服务传递的快照比当前快照旧，忽略它
	if index <= snapshotIndex {
		log.Printf("服务器[%d] 忽略过期快照，索引=%d，当前最后包含索引=%d",
			rf.me, index, snapshotIndex)
		return
	}

	// index > snapshotIndex时
	newLogs := make([]LogEntry, len(rf.logs[index-snapshotIndex:]))
	copy(newLogs, rf.logs[index-snapshotIndex:])

	// 设置第一个条目作为空占位符（表示快照点）
	newLogs[0] = LogEntry{
		Index:   index,
		Term:    rf.logs[index-snapshotIndex].Term,
		Command: "",
	}
	rf.logs = newLogs

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	log.Printf("在接受索引为 %v 的快照后，节点 %v 的状态为：{状态：%v, 任期: %v, 提交索引: %v, 已应用索引: %v, 首条日志: %v, 末条日志：%v}",
		index, rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer log.Printf("在处理完 InstallSnapshot后，服务器[%v]的状态是 {state: %v, term: %v}", rf.me, rf.state, rf.currentTerm)
	defer log.Printf("本次InstallSnapshot中, args: %v, reply: %v", args, reply)

	reply.Term = rf.currentTerm

	// 领导者任期落后，立即返回
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}

	rf.changeState(FOLLOWER)
	rf.electionTimer.Reset(GenElectionTimeOut())

	// 检查快照是否过时
	if args.LastIncludedIndex <= rf.commitIndex {
		log.Printf("由于当前的commitIndex [%v]在任期 %v 中更大，服务器[%v]拒绝了 lastIncludeIndex 为[%v]的旧快照...\n",
			rf.commitIndex, rf.currentTerm, rf.me, args.LastIncludedIndex)
		return
	}

	// 更新日志状态
	if args.LastIncludedIndex > rf.getLastLog().Index { //在索引为0的地方需要哨兵日志 need dummy entry at index 0
		rf.logs = make([]LogEntry, 1)
	} else {
		newLogs := make([]LogEntry, len(rf.logs[args.LastIncludedIndex-rf.getFirstLog().Index:]))
		copy(newLogs, rf.logs[args.LastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs = newLogs
		rf.logs[0].Command = nil
	}

	// 更新索引和任期信息
	rf.logs[0].Term = args.LastIncludedTerm
	rf.logs[0].Index = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// 持久化状态和快照
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)

	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	log.Printf("服务器[%v]已成功安装快照，快照索引=%v，快照任期=%v",
		rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct { // AppendEntries RPC的参数
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct { // AppendEntries RPC的返回值
	Term    int
	Success bool
	XTerm   int // follower中包含冲突条目的任期
	XIndex  int //冲突条目所在任期的第一个索引
	XLen    int // 日志长度
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("服务器 [%d]收到AppendEntries RPC，其中args参数信息：%+v...\n", rf.me, args)
	log.Printf("服务器 [%d]当前rf.currentTerm: %d\n", rf.me, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		log.Println("false返回点1")
		return
	}

	// 如果Leader的任期更大，更新自己的任期并转为Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}

	rf.changeState(FOLLOWER)
	rf.electionTimer.Reset(GenElectionTimeOut())

	// 如果日志在 prevLogIndex 位置不包含一个任期与 prevLogTerm 匹配的条目，则回复 false
	if args.PrevLogIndex < rf.getFirstLog().Index {

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 检查日志是否匹配，如果不匹配，则返回冲突的索引和任期
	// 如果现有日志条目与新条目冲突（相同的索引但不同的任期），则删除该现有条目及其之后的所有条目
	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false

		lastLogIndex := rf.getLastLog().Index
		// 找到冲突任期的第一个索引
		if lastLogIndex < args.PrevLogIndex {
			reply.XIndex = lastLogIndex
			reply.XTerm = -1
		} else {
			firstLogIndex := rf.getFirstLog().Index
			//找到冲突任期中的第一个索引
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			reply.XIndex = index + 1
			reply.XTerm = args.PrevLogTerm
		}
		return
	}

	// 将之前没有的新日志添加到rf.logs中
	firstLogIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		if entry.Index-firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index-firstLogIndex], args.Entries[index:]...)
			rf.persist()
			break
		}
	}

	// 如果 leaderCommit > commitIndex，将 commitIndex 设置为 min(leaderCommit, 最后一个新条目的索引)
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		log.Printf("{节点 %v} 在任期 %v 中，将 commitIndex 从 %v 提升到 %v", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm { // candidate的term更大，本节点转变为follower，但是candidate不一定就是leader（还没有通过选举限制）
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.voteFor = -1 // 本节点转变为未投票状态

		rf.persist()
	}

	// 如果候选者的日志不够新，则拒接投票
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.voteFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(GenElectionTimeOut())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1      // 该命令被提交后的日志索引
	term := -1       // 当前任期号
	isLeader := true // 表示本服务器自认为是leader

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		isLeader = false

		return index, term, isLeader
	}

	// 当前服务器是LEADER，首先需要将新的日志追加到本地并持久化，然后发送AppendEntries给其他服务器
	newLogIndex := rf.getLastLog().Index + 1
	newLog := LogEntry{
		Index:   newLogIndex,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, newLog)

	rf.persist() // 接收到新日志时，立即进行持久化

	log.Printf("领导者 [%v] 接收到新日志： %+v\n", rf.me, newLog)

	index = newLogIndex
	term = rf.currentTerm

	rf.matchIndex[rf.me] = newLogIndex
	rf.nextIndex[rf.me] = newLogIndex + 1

	rf.broadcastHeartbeats(false)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// 检查Raft节点日志中指定索引位置的条目是否与给定任期匹配
func (rf *Raft) isLogMatched(index, term int) bool {
	// 比较任期时，有一个逻辑索引转换为物理索引的过程
	return index <= rf.getLastLog().Index && term == rf.logs[index-rf.getFirstLog().Index].Term
}

func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	// 获取已知在大多数服务器上复制的、索引最高的日志条目的索引
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.isLogMatched(newCommitIndex, rf.currentTerm) {

			log.Printf("服务器[%v] 在任期 %v 中将 commitIndex 从 %v 提升到 %v...\n", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) replicateOnceRound(server int) {
	rf.mu.RLock()

	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// 只发送 InstallSnapshot RPC
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(server, &args, &reply) {
			rf.mu.Lock()
			if rf.state == LEADER && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.changeState(FOLLOWER)
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.persist()
				} else {
					rf.nextIndex[server] = args.LastIncludedIndex + 1
					rf.matchIndex[server] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
			log.Printf("Leader [%v] 发送快照 %+v 给 服务器[%v]，并收到Reply %v", rf.me, args, server, reply)
		}
	} else { // 日志复制
		args := rf.genAppendEntriesArgs(prevLogIndex)

		rf.mu.RUnlock()
		reply := AppendEntriesReply{}

		if prevLogIndex+1 <= rf.getLastLog().Index {
			log.Printf("Leader[%d] 开始发送日志复制RPC: 其中日志索引从[%d] 到[%d]，LeaderCommit为[%d]...\n", rf.me, prevLogIndex+1, rf.getLastLog().Index, rf.commitIndex)

		}

		ok := rf.sendAppendEntries(server, &args, &reply)

		if ok {
			rf.mu.Lock()
			if args.Term == rf.currentTerm && rf.state == LEADER {
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						// 不成功的原因是，对方有更高的任期
						rf.changeState(FOLLOWER)
						rf.currentTerm = reply.Term
						rf.voteFor = -1

						rf.persist()

						log.Printf("领导者 %d 发现服务器%d 存在更高任期为：%d，转变为follower", rf.me, server, reply.Term)
					} else if reply.Term <= rf.currentTerm { // 不成功的原因是日志冲突
						// 减小nextIndex并重试
						rf.nextIndex[server] = reply.XIndex
						if reply.Term != -1 {
							firstLogIndex := rf.getFirstLog().Index
							// 找到最大的索引 i，使得索引为 i 的日志条目的任期号小于等于 reply.XTerm
							lo, hi := firstLogIndex, args.PrevLogIndex-1
							for lo < hi {
								mid := (lo + hi + 1) / 2
								if rf.logs[mid-firstLogIndex].Term <= reply.XTerm {
									lo = mid
								} else {
									hi = mid - 1
								}
							}
							if rf.logs[lo-firstLogIndex].Term == reply.XTerm {
								rf.nextIndex[server] = lo
							}
						}

					}

				} else {
					// 复制成功
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					// advance commitIndex if possible
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) broadcastHeartbeats(isHeartbeat bool) {

	for server := range rf.peers {

		if server == rf.me {
			continue
		}

		if isHeartbeat {
			// 应该立即发送心跳给所有服务器
			log.Printf("Leader [%d] 开始发送心跳给服务器 [%d]...", rf.me, server)
			go rf.replicateOnceRound(server)
		} else {
			// 只需要进行日志复制
			rf.replicatorCond[server].Signal()
		}

	}
}

func (rf *Raft) startElection() {
	// 开始选举
	log.Printf("服务器%d 开始选举...\n", rf.me)

	rf.voteFor = rf.me // 投票给自己
	rf.persist()

	// 投票参数
	votesReceived := 1 // 包含自己的一票
	majorityNum := len(rf.peers)/2 + 1

	args := rf.genRequestVoteArgs()

	// 向其他所有服务器发送RequestVote RPC
	for server := range rf.peers { // 这里的server是指rf.peers的index，也就是每个server的id
		if server == rf.me {
			continue
		}
		if rf.state != CANDIDATE {
			return
		}

		// 为每个服务器启动一个goroutine
		go func(server int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			log.Printf("服务器 [%d] 向服务器[%d]请求投票给自己...\n", args.CandidateId, server)
			log.Printf("服务器[%d]响应结果，ok: %v, rf.currentTerm: %v, reply.Term: %v, reply.VoteGranted: %v", server, ok, rf.currentTerm, reply.Term, reply.VoteGranted)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				log.Printf("服务器 [%d] 收到来自 服务器[%d]的requestVote reply...\n", args.CandidateId, server)
				if reply.Term > rf.currentTerm { // reply.Term > rf.currentTerm特殊处理

					rf.changeState(FOLLOWER)
					rf.currentTerm = reply.Term
					rf.voteFor = -1

					rf.persist()

				} else if rf.state == CANDIDATE { // reply.Term <= rf.currentTerm
					if reply.VoteGranted {
						votesReceived++
						if votesReceived >= majorityNum && rf.state == CANDIDATE { // 成为Leader

							// 变更角色
							log.Printf("服务器 [%d ] 成为Leader...", rf.me)
							rf.changeState(LEADER)
							rf.broadcastHeartbeats(true)
						}

					}

				}

			}

		}(server, args)

	}

	rf.persist()

}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		// 检查commitIndex是否已经前进
		for rf.commitIndex <= rf.lastApplied {
			// // 需要等待commitIndex被提交
			rf.applyCond.Wait()
		}

		// rf.commitIndex > rf.lastApplied
		firstLogIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex // 提前记录 commitIndex
		lastApplied := rf.lastApplied

		entries := make([]LogEntry, commitIndex-lastApplied)
		// 包含一个逻辑索引到物理索引的转换
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		rf.mu.Lock()
		log.Printf("服务器[ %d ] 现在向applyCh通道提交了任期为 %d ，索引从 %d 到 %d 的日志\n", rf.me, rf.currentTerm, lastApplied+1, commitIndex)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

	}
}

// 检查 指定server 是否需要接收日志复制
func (rf *Raft) needReplicating(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// 比较该节点的matchIndex与Leader最新日志索引来判断其是否落后
	return rf.state == LEADER && rf.matchIndex[server] < rf.getLastLog().Index
}

// 为 指定server 服务器持续运行的日志复制器
func (rf *Raft) replicator(server int) {
	rf.replicatorCond[server].L.Lock()
	defer rf.replicatorCond[server].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(server) {
			rf.replicatorCond[server].Wait()
		}
		// 将日志赋值给 指定server
		rf.replicateOnceRound(server)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.

		select {
		case <-rf.electionTimer.C: // 选举超时时间内未收到心跳，转变为candidate进行选举

			rf.mu.Lock()
			rf.changeState(CANDIDATE)
			rf.currentTerm++
			rf.electionTimer.Reset(GenElectionTimeOut())
			rf.persist()

			// 只有在非Leader状态下才开始选举
			if rf.state != LEADER {
				rf.startElection()
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C: // 如果是领导者则发送心跳
			rf.mu.Lock()

			if rf.state == LEADER {
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
				rf.broadcastHeartbeats(true)
			}

			rf.mu.Unlock()

		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		mu:        sync.RWMutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		voteFor:     -1, // 代表null
		logs: []LogEntry{
			{
				Index:   0,
				Term:    0,
				Command: "",
			},
		},

		state: FOLLOWER,

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		electionTimer:  time.NewTimer(GenElectionTimeOut()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),

		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		// Your initialization code here (3A, 3B, 3C).
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.replicatorCond = make([]*sync.Cond, len(peers))

	// 为每一个peer创建一个 日志复制的 专用条件变量
	for server := range peers {
		rf.matchIndex[server] = 0
		rf.nextIndex[server] = rf.getLastLog().Index + 1

		if server != rf.me {
			rf.replicatorCond[server] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(server)
		}
	}

	// start ticker goroutine to start elections （每隔一段时间检查是否收到了心跳）
	go rf.ticker()

	go rf.applier()

	return rf
}
