package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey   string // 锁在KV存储中的键名
	lockOwner string // 本客户端的唯一标识符
	acquired  bool   // 本客户端是否持有锁
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
		ck:        ck,
		lockKey:   l,
		lockOwner: kvtest.RandValue(8),
		acquired:  false,
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	if lk.acquired {
		return // 锁已经获取过了
	}

	for {
		// 查看锁是否被获取
		value, version, err := lk.ck.Get(lk.lockKey) // Get()有可能因网络问题失败，没有返回值

		// 如果锁不存在，使用版本号0获取锁
		if err == rpc.ErrNoKey {
			putErr := lk.ck.Put(lk.lockKey, lk.lockOwner, 0) // 要检查put的结果
			if putErr == rpc.OK {
				lk.acquired = true
				log.Printf("[客户端-%s] 获取锁成功...\n", lk.lockOwner)
				return
			} else if putErr == rpc.ErrMaybe {
				checkVal, _, _ := lk.ck.Get(lk.lockKey)
				if checkVal == lk.lockOwner { // 这里是版本号0获取锁，所以只有可能“锁已成功获取，但是OK响应丢失”这种情形
					// 确认锁确实已被我们获取
					lk.acquired = true
					log.Printf("ErrMaybe情况下发现锁已被 [客户端-%s] 被获取...\n", lk.lockOwner)
					return
				}
			}

		} else if err == rpc.OK && value == "" { // 如果锁存在但是未被持有，那么使用当前版本号获取
			putErr := lk.ck.Put(lk.lockKey, lk.lockOwner, version)
			if putErr == rpc.OK {
				lk.acquired = true
				log.Printf("[客户端-%s] 获取锁成功...\n", lk.lockOwner)
				return

			} else if putErr == rpc.ErrMaybe { // 这里可能为ErrMaybe，ErrMaybe存在两种结果
				// 结果1：锁已成功获取，但是响应丢失
				checkVal, _, _ := lk.ck.Get(lk.lockKey)
				if checkVal == lk.lockOwner {
					// 确认锁确实已被我们获取
					lk.acquired = true
					log.Printf("ErrMaybe情况下发现锁已被 [客户端-%s] 被获取...\n", lk.lockOwner)
					return
				}
				// 锁获取确实失败
				log.Printf("ErrMaybe情况下发现锁已被获取，[客户端-%s]获取失败...\n", lk.lockOwner)

				continue

			}

		}

		// 锁已被其他客户端获取，等待100 ms
		log.Printf("[客户端-%s] 获取锁失败，等待100ms...\n", lk.lockOwner)
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	if !lk.acquired { // 没有持有锁，直接返回
		return
	}
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockKey) // Get()可能因网络问题失败
		if err != rpc.OK && err != rpc.ErrNoKey {
			log.Printf("[客户端-%s] 在释放锁前获取锁状态失败: %v，重试中...", lk.lockOwner, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err == rpc.OK && value == lk.lockOwner { // 是自己持有的锁，才释放
			PutErr := lk.ck.Put(lk.lockKey, "", version)
			if PutErr == rpc.OK { // 要检查Put的结果
				log.Printf("[客户端-%s] 释放锁成功...", lk.lockOwner)
				lk.acquired = false
				return

			} else if PutErr == rpc.ErrMaybe { // 返回Maybe的不同情况
				checkVal, _, _ := lk.ck.Get(lk.lockKey)
				if checkVal == "" {
					// 情况1·=：确认锁确实已被我们释放，只是响应丢失
					log.Printf("[客户端-%s] ErrMaybe情况下确认锁已释放\n", lk.lockOwner)
					lk.acquired = false
					return
				} else if checkVal != "" && checkVal != lk.lockOwner { // 情况2：释放的响应丢失，然后其他客户端获取了锁，不需要管
					log.Printf("[客户端-%s] ErrMaybe情况下确认锁已释放\n", lk.lockOwner)
					lk.acquired = false
					return
				} else { // 情况3：checkVal == lk.lockOwner的情形。可能是意外失败，重试
					continue
				}
			}
			log.Printf("[客户端-%s] 释放锁失败...\n", lk.lockOwner)
			continue

		} else { // 锁不属于我们，返回
			log.Printf("[客户端-%s] 尝试释放非自己持有的锁，视为已释放...", lk.lockOwner)
			lk.acquired = false
			return

		}
	}
}
