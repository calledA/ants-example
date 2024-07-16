package ants

import "time"

// stack类型的worker
type workerStack struct {
	items  []worker
	expiry []worker
}

// 根据size新建worker
func newWorkerStack(size int) *workerStack {
	return &workerStack{
		items: make([]worker, 0, size),
	}
}

// 当前workerStack长度
func (wq *workerStack) len() int {
	return len(wq.items)
}

// 判断workerStack是否为空
func (wq *workerStack) isEmpty() bool {
	return len(wq.items) == 0
}

// workerStack插入数据
func (wq *workerStack) insert(w worker) error {
	wq.items = append(wq.items, w)
	return nil
}

// 返回workerStack最后一位的数据
func (wq *workerStack) detach() worker {
	l := wq.len()
	if l == 0 {
		return nil
	}

	w := wq.items[l-1]
	// 避免内存泄露
	wq.items[l-1] = nil
	wq.items = wq.items[:l-1]

	return w
}

// 清理过期的worker
func (wq *workerStack) refresh(duration time.Duration) []worker {
	n := wq.len()
	if n == 0 {
		return nil
	}

	// 通过expiryTime找到要过期的worker
	expiryTime := time.Now().Add(-duration)
	index := wq.binarySearch(0, n-1, expiryTime)

	// 清空[]expiry
	wq.expiry = wq.expiry[:0]
	// 判断是否有过期的worker
	if index != -1 {
		// 添加过期的worker到[]expiry
		wq.expiry = append(wq.expiry, wq.items[:index+1]...)
		// 复制未过期的worker
		m := copy(wq.items, wq.items[index+1:])
		// 将[]items过期的worker清空，防止内存泄露
		for i := m; i < n; i++ {
			wq.items[i] = nil
		}
		// 重新分配[]items空间
		wq.items = wq.items[:m]
	}
	// 返回过期的worker
	return wq.expiry
}

// 二分查找过期的worker
func (wq *workerStack) binarySearch(l, r int, expiryTime time.Time) int {
	var mid int
	for l <= r {
		mid = (l + r) / 2
		// 通过判断过期时间，选择边界值
		if expiryTime.Before(wq.items[mid].lastUsedTime()) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return r
}

// 重设workerStack
func (wq *workerStack) reset() {
	// 先finish worker的task，再清空worker
	for i := 0; i < wq.len(); i++ {
		wq.items[i].finish()
		wq.items[i] = nil
	}
	wq.items = wq.items[:0]
}
