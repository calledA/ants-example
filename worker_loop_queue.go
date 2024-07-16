package ants

import "time"

// loopQueue类型的worker
type loopQueue struct {
	items  []worker
	expiry []worker
	head   int
	tail   int
	size   int
	isFull bool
}

// 根据size新建worker
func newWorkerLoopQueue(size int) *loopQueue {
	return &loopQueue{
		items: make([]worker, size),
		size:  size,
	}
}

// 当前loopQueue长度
func (wq *loopQueue) len() int {
	// 判断非空
	if wq.size == 0 {
		return 0
	}

	// 如果头尾相连，判断是否full
	if wq.head == wq.tail {
		// 如果full，则loopQueue闭环
		if wq.isFull {
			return wq.size
		}
		// 返回0
		return 0
	}

	// 如果尾>头，尾-头的长度即loopQueue的长度
	if wq.tail > wq.head {
		return wq.tail - wq.head
	}

	// 如果尾<头，需要通过size来确定len
	return wq.size - wq.head + wq.tail
}

// 判断loopQueue是否为空
func (wq *loopQueue) isEmpty() bool {
	// 头尾相连但没有full
	return wq.head == wq.tail && !wq.isFull
}

// loopQueue插入数据
func (wq *loopQueue) insert(w worker) error {
	if wq.size == 0 {
		return errQueueIsReleased
	}

	if wq.isFull {
		return errQueueIsFull
	}
	// 如果size!=0，且未full，则说明有空间可以直接插入
	wq.items[wq.tail] = w
	wq.tail++

	// 如果tail==size，则回到第0位
	if wq.tail == wq.size {
		wq.tail = 0
	}
	// 如果头尾相连，则设置full
	if wq.tail == wq.head {
		wq.isFull = true
	}

	return nil
}

// 返回loopQueue头位的数据
func (wq *loopQueue) detach() worker {
	if wq.isEmpty() {
		return nil
	}

	// 获取head数据
	w := wq.items[wq.head]
	// 清空内存，防止内存泄露
	wq.items[wq.head] = nil
	wq.head++
	// 如果head==size，则回到第0位
	if wq.head == wq.size {
		wq.head = 0
	}
	wq.isFull = false

	return w
}

// 清理过期的worker
func (wq *loopQueue) refresh(duration time.Duration) []worker {
	// 通过expiryTime找到要过期的worker
	expiryTime := time.Now().Add(-duration)
	index := wq.binarySearch(expiryTime)
	if index == -1 {
		return nil
	}

	// 清空[]expiry
	wq.expiry = wq.expiry[:0]

	// 分情况处理，当head<=index，则为正序直接添加，否则切片截取数据
	if wq.head <= index {
		// 添加worker到[]expiry
		wq.expiry = append(wq.expiry, wq.items[wq.head:index+1]...)
		// 清空内存，防止内存泄露
		for i := wq.head; i < index+1; i++ {
			wq.items[i] = nil
		}
	} else {
		// 从中间截取数据
		wq.expiry = append(wq.expiry, wq.items[0:index+1]...)
		wq.expiry = append(wq.expiry, wq.items[wq.head:]...)
		// 清空内存，防止内存泄露
		for i := 0; i < index+1; i++ {
			wq.items[i] = nil
		}
		for i := wq.head; i < wq.size; i++ {
			wq.items[i] = nil
		}
	}

	// 获取loopQueue的head
	head := (index + 1) % wq.size
	wq.head = head
	if len(wq.expiry) > 0 {
		wq.isFull = false
	}

	// 返回过期的worker
	return wq.expiry
}

// 二分查找过期的worker
func (wq *loopQueue) binarySearch(expiryTime time.Time) int {
	var mid, nlen, basel, tmid int
	nlen = len(wq.items)

	// 如果队列为空或者第一个元素的过期时间大于给定的过期时间，返回 -1
	if wq.isEmpty() || expiryTime.Before(wq.items[wq.head].lastUsedTime()) {
		return -1
	}

	// 队列示例：展示了头尾指针在队列中的映射位置
	// size = 8, head = 7, tail = 4
	// [ 2, 3, 4, 5, nil, nil, nil,  1]  true position
	//   0  1  2  3    4   5     6   7
	//              tail          head
	//
	//   1  2  3  4  nil nil   nil   0   mapped position
	//            r                  l

	// 将头尾映射到有效的左边和右边
	r := (wq.tail - 1 - wq.head + nlen) % nlen
	basel = wq.head
	l := 0
	for l <= r {
		mid = l + ((r - l) >> 1)
		// 计算从映射中得出的真正的中间位置
		tmid = (mid + basel + nlen) % nlen
		if expiryTime.Before(wq.items[tmid].lastUsedTime()) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	// 返回真正位置
	return (r + basel + nlen) % nlen
}

// 重设workerStack
func (wq *loopQueue) reset() {
	if wq.isEmpty() {
		return
	}

retry:
	// 先finish worker的task，再清空worker
	if w := wq.detach(); w != nil {
		w.finish()
		goto retry
	}
	wq.items = wq.items[:0]
	wq.size = 0
	wq.head = 0
	wq.tail = 0
}
