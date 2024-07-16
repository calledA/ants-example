// Copyright 2019 Andy Pan & Dietoad. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sync

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type spinLock uint32

const maxBackoff = 16

// 自旋锁是一种忙等待锁，在锁被占用时，线程会在一个循环中反复检查锁是否已经释放，而不是阻塞或休眠
func (sl *spinLock) Lock() {
	// 回避次数初始化
	backoff := 1
	// 使用原子操作获取锁，如果能swap，则能获取到锁
	for !atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
		// 回避次数
		for i := 0; i < backoff; i++ {
			// 让出当前线程的执行权，允许其他线程运行
			runtime.Gosched()
		}
		// 上一个回避次数之前不能获取到锁，则进行指数退避，但不会超过maxBackoff
		if backoff < maxBackoff {
			backoff <<= 1
		}
	}
}

// 自旋锁Unlock
func (sl *spinLock) Unlock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}

// 初始化自旋锁
func NewSpinLock() sync.Locker {
	return new(spinLock)
}
