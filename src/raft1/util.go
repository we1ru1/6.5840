package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const ELECTION_TIMEOUT = 1000
const HEARTBEAT_INTERVAL = 125 // 单位是 ms

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) genRequestVoteArgs() RequestVoteArgs { //用于直接产生RequestVoteArgs RPC的参数
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	return args
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	// 为什么这里是prevLogIndex-firstLogIndex + 1: ？
	// 在使用快照功能后，有一个物理索引和逻辑索引的转换
	// 		假定firstLogIndex为5，prevLogIndex逻辑索引号为8
	//		那么 8 - 5 = 3，即prevLogIndex在数组中实际位置是rf.logs[3]，那么要复制的第一个位置应该是4
	// 公式就是 prevLogIndex物理索引号 = prevLogIndex逻辑索引号 - firstLogIndex物理索引号
	// 现在我们要复制的日志开头是 prevLogIndex之后第一个位置，所以 +1
	entries := make([]LogEntry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex-firstLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}

func (rf *Raft) genInstallSnapshotArgs() InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

func Min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a <= b {
		return b
	} else {
		return a
	}
}

func GenElectionTimeOut() time.Duration { // 随机产生 选举超时时间
	rand_ms := ELECTION_TIMEOUT + (rand.Int63() % ELECTION_TIMEOUT) // 选举超时时间设置为1000 ~ 2000 ms的随机值
	return time.Duration(rand_ms) * time.Millisecond

}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond
}
