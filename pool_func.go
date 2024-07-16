package ants

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	syncx "github.com/panjf2000/ants/v2/internal/sync"
)

// pool结构（携带poolFunc）
type PoolWithFunc struct {
	// pool的容量，负数代表是无限容量，避免嵌套使用pool
	capacity int32

	// 当前running的goroutine
	running int32

	// worker的锁
	lock sync.Locker

	// 存储可用worker
	workers workerQueue

	// 用于通知关闭pool
	state int32

	// 等待获取空闲的worker
	cond *sync.Cond

	// poolFunc is the function for processing tasks.
	poolFunc func(interface{})

	// retrieveWorker获取worker的缓存
	workerCache sync.Pool

	// pool.Submit()提交的阻塞的goroutine
	waiting int32

	// 定期清理worker
	purgeDone int32
	stopPurge context.CancelFunc

	// 定期更新当前时间
	ticktockDone int32
	stopTicktock context.CancelFunc

	// 记录执行时间
	now atomic.Value

	// options配置
	options *Options
}

// 定期清理worker，作为goroutine运行
func (p *PoolWithFunc) purgeStaleWorkers(ctx context.Context) {
	// 通过options.ExpiryDuration设置的间隔时间进行清理worker
	ticker := time.NewTicker(p.options.ExpiryDuration)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.purgeDone, 1)
	}()

	// 阻塞channel
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		// 找到过期的worker
		p.lock.Lock()
		staleWorkers := p.workers.refresh(p.options.ExpiryDuration)
		p.lock.Unlock()

		// 停止过期的worker，此操作在p.lock之外，因为很多worker在非本地的CPU。使用w.task阻塞会浪费很多时间
		for i := range staleWorkers {
			staleWorkers[i].finish()
			staleWorkers[i] = nil
		}

		// 当所有worker都被清理（没有worker在running）或者pool的容量调整，需要唤醒p.cond.Wait()阻塞的所有worker
		if p.Running() == 0 || (p.Waiting() > 0 && p.Free() > 0) {
			p.cond.Broadcast()
		}
	}
}

// 定期更新pool当前时间的goroutine
func (p *PoolWithFunc) ticktock(ctx context.Context) {
	ticker := time.NewTicker(nowTimeUpdateInterval)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.ticktockDone, 1)
	}()

	// 阻塞channel
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		// 更新当前时间
		p.now.Store(time.Now())
	}
}

// 启动purgeStaleWorkers协程
func (p *PoolWithFunc) goPurge() {
	if p.options.DisablePurge {
		return
	}

	// 启动协程定时清除过期的worker
	var ctx context.Context
	ctx, p.stopPurge = context.WithCancel(context.Background())
	go p.purgeStaleWorkers(ctx)
}

// 启动ticktock协程
func (p *PoolWithFunc) goTicktock() {
	p.now.Store(time.Now())
	var ctx context.Context
	ctx, p.stopTicktock = context.WithCancel(context.Background())
	go p.ticktock(ctx)
}

// 获取当前时间
func (p *PoolWithFunc) nowTime() time.Time {
	return p.now.Load().(time.Time)
}

// 创建ants pool实例
func NewPoolWithFunc(size int, pf func(interface{}), options ...Option) (*PoolWithFunc, error) {
	if size <= 0 {
		size = -1
	}

	if pf == nil {
		return nil, ErrLackPoolFunc
	}

	// 加载options配置
	opts := loadOptions(options...)

	// 设置清理时间
	if !opts.DisablePurge {
		if expiry := opts.ExpiryDuration; expiry < 0 {
			return nil, ErrInvalidPoolExpiry
		} else if expiry == 0 {
			opts.ExpiryDuration = DefaultCleanIntervalTime
		}
	}

	// 设置Logger
	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}

	// 创建pool实例（携带poolFunc）
	p := &PoolWithFunc{
		capacity: int32(size),
		poolFunc: pf,
		lock:     syncx.NewSpinLock(),
		options:  opts,
	}

	// 创建worker实例
	p.workerCache.New = func() interface{} {
		return &goWorkerWithFunc{
			pool: p,
			args: make(chan interface{}, workerChanCap),
		}
	}

	// 设置[]worker
	if p.options.PreAlloc {
		if size == -1 {
			return nil, ErrInvalidPreAllocSize
		}
		// 创建queue类型的[]worker
		p.workers = newWorkerArray(queueTypeLoopQueue, size)
	} else {
		// 创建stack类型的[]worker
		p.workers = newWorkerArray(queueTypeStack, 0)
	}

	// 设置cond实例
	p.cond = sync.NewCond(p.lock)

	// 启动清理goroutine
	p.goPurge()
	p.goTicktock()

	return p, nil
}

//---------------------------------------------------------------------------

// Invoke submits a task to pool.
//
// Note that you are allowed to call Pool.Invoke() from the current Pool.Invoke(),
// but what calls for special attention is that you will get blocked with the latest
// Pool.Invoke() call once the current Pool runs out of its capacity, and to avoid this,
// you should instantiate a PoolWithFunc with ants.WithNonblocking(true).
func (p *PoolWithFunc) Invoke(args interface{}) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}
	if w := p.retrieveWorker(); w != nil {
		w.inputParam(args)
		return nil
	}
	return ErrPoolOverload
}

// Running returns the number of workers currently running.
func (p *PoolWithFunc) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free returns the number of available goroutines to work, -1 indicates this pool is unlimited.
func (p *PoolWithFunc) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// Waiting returns the number of tasks which are waiting be executed.
func (p *PoolWithFunc) Waiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// Cap returns the capacity of this pool.
func (p *PoolWithFunc) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *PoolWithFunc) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
	}
}

// IsClosed indicates whether the pool is closed.
func (p *PoolWithFunc) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// Release closes this pool and releases the worker queue.
func (p *PoolWithFunc) Release() {
	if !atomic.CompareAndSwapInt32(&p.state, OPENED, CLOSED) {
		return
	}
	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast()
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
func (p *PoolWithFunc) ReleaseTimeout(timeout time.Duration) error {
	if p.IsClosed() || (!p.options.DisablePurge && p.stopPurge == nil) || p.stopTicktock == nil {
		return ErrPoolClosed
	}

	if p.stopPurge != nil {
		p.stopPurge()
		p.stopPurge = nil
	}
	p.stopTicktock()
	p.stopTicktock = nil
	p.Release()

	endTime := time.Now().Add(timeout)
	for time.Now().Before(endTime) {
		if p.Running() == 0 &&
			(p.options.DisablePurge || atomic.LoadInt32(&p.purgeDone) == 1) &&
			atomic.LoadInt32(&p.ticktockDone) == 1 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ErrTimeout
}

// Reboot reboots a closed pool.
func (p *PoolWithFunc) Reboot() {
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		atomic.StoreInt32(&p.purgeDone, 0)
		p.goPurge()
		atomic.StoreInt32(&p.ticktockDone, 0)
		p.goTicktock()
	}
}

//---------------------------------------------------------------------------

func (p *PoolWithFunc) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

func (p *PoolWithFunc) addWaiting(delta int) {
	atomic.AddInt32(&p.waiting, int32(delta))
}

// retrieveWorker returns an available worker to run the tasks.
func (p *PoolWithFunc) retrieveWorker() (w worker) {
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorkerWithFunc)
		w.run()
	}

	p.lock.Lock()
	w = p.workers.detach()
	if w != nil { // first try to fetch the worker from the queue
		p.lock.Unlock()
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() {
		// if the worker queue is empty and we don't run out of the pool capacity,
		// then just spawn a new worker goroutine.
		p.lock.Unlock()
		spawnWorker()
	} else { // otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.
		if p.options.Nonblocking {
			p.lock.Unlock()
			return
		}
	retry:
		if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
			p.lock.Unlock()
			return
		}

		p.addWaiting(1)
		p.cond.Wait() // block and wait for an available worker
		p.addWaiting(-1)

		if p.IsClosed() {
			p.lock.Unlock()
			return
		}

		var nw int
		if nw = p.Running(); nw == 0 { // awakened by the scavenger
			p.lock.Unlock()
			spawnWorker()
			return
		}
		if w = p.workers.detach(); w == nil {
			if nw < p.Cap() {
				p.lock.Unlock()
				spawnWorker()
				return
			}
			goto retry
		}
		p.lock.Unlock()
	}
	return
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *PoolWithFunc) revertWorker(worker *goWorkerWithFunc) bool {
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}

	worker.lastUsed = p.nowTime()

	p.lock.Lock()
	// To avoid memory leaks, add a double check in the lock scope.
	// Issue: https://github.com/panjf2000/ants/issues/113
	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}
	if err := p.workers.insert(worker); err != nil {
		p.lock.Unlock()
		return false
	}
	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	p.cond.Signal()
	p.lock.Unlock()

	return true
}
