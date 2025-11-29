package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// 当每个 Raft 节点意识到连续的日志条目已被提交时，
// 该节点应该通过传递给 Make() 的 applyCh 向服务器（或测试程序）
// 发送一个 ApplyMsg。将 CommandValid 设置为 true，
// 以表明 ApplyMsg 包含一个新的已提交日志条目。
//
// 在实验 3 中，你可能需要在 applyCh 上发送其他类型的消息（例如// 快照）；
// 在那个时候，你可以向 ApplyMsg 添加字段，
// 但对于这些其他用途，请将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
