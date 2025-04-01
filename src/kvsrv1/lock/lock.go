package lock

import (
	kvtest "6.5840/kvtest1"
	"github.com/google/uuid"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey string		// 锁在KV存储中的键名
	lockOwner string	// 本客户端的唯一标识符
	acquired bool 		// 本客户端是否持有锁
	// 分布式锁，每个客户端都有一个Lock实例，锁的状态存储在共享存储中
	// 指向某个客户端的指针，表示这个锁已被那个客户端拿到
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	// lk := &Lock{ck: ck}
	// You may add code here
	lk := &Lock{
		ck: ck,
		lockKey: l,
		lockOwner: uuid.New().String(),
		acquired: false,
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// 查看锁是否被获取

	lk.ck.Get(lk.)

}

func (lk *Lock) Release() {
	// Your code here
	// 当前lk是否拿到了锁，如果拿到就释放，
	// 否则返回，并没哟持有锁
}
