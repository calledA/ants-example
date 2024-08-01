package ants

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	syncx "github.com/panjf2000/ants/v2/internal/sync"
)

// pool结构
type Pool struct {
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
func (p *Pool) purgeStaleWorkers(ctx context.Context) {
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
func (p *Pool) ticktock(ctx context.Context) {
	// 定义定时器
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
func (p *Pool) goPurge() {
	if p.options.DisablePurge {
		return
	}

	// 启动协程定时清除过期的worker
	var ctx context.Context
	ctx, p.stopPurge = context.WithCancel(context.Background())
	go p.purgeStaleWorkers(ctx)
}

// 启动ticktock协程
func (p *Pool) goTicktock() {
	p.now.Store(time.Now())
	var ctx context.Context
	ctx, p.stopTicktock = context.WithCancel(context.Background())
	go p.ticktock(ctx)
}

// 获取当前时间
func (p *Pool) nowTime() time.Time {
	return p.now.Load().(time.Time)
}

// 创建ants pool实例
func NewPool(size int, options ...Option) (*Pool, error) {
	if size <= 0 {
		size = -1
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

	// 创建pool实例
	p := &Pool{
		capacity: int32(size),
		lock:     syncx.NewSpinLock(),
		options:  opts,
	}

	// 创建worker实例
	p.workerCache.New = func() interface{} {
		return &goWorker{
			pool: p,
			task: make(chan func(), workerChanCap),
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

// ---------------------------------------------------------------------------

// 当pool容量耗尽时，最新的pool.Submit()将会阻塞，为了避免这种情况，应该设置ants.WithNonblocking(true)
func (p *Pool) Submit(task func()) error {
	// 判断pool状态
	if p.IsClosed() {
		return ErrPoolClosed
	}
	// TODO
	if w := p.retrieveWorker(); w != nil {
		w.inputFunc(task)
		return nil
	}
	return ErrPoolOverload
}

// 返回当前running的worker
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// 返回可用的goroutine数量，-1代表poll容量无限
func (p *Pool) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running()
}

// 返回等待执行的task数量
func (p *Pool) Waiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// 返回pool的容量
func (p *Pool) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// 调整池的容量，对预分配池和无限量的池无效
func (p *Pool) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	// 使用原子操作，避免并发问题
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		// 启动waiting的worker
		if size-capacity == 1 {
			p.cond.Signal()
			return
		}
		p.cond.Broadcast()
	}
}

// 返回pool是否被关闭
func (p *Pool) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// 清空[]worker并关闭pool
func (p *Pool) Release() {
	// 关闭pool并重设
	if !atomic.CompareAndSwapInt32(&p.state, OPENED, CLOSED) {
		return
	}
	p.lock.Lock()
	p.workers.reset()
	p.lock.Unlock()
	// 将等待的worker全部唤醒，防止关闭pool之后无限等待
	p.cond.Broadcast()
}

// 和Release类似，ReleaseTimeout添加一个等待时间timeout，超过timeout仍未完成所有task，返回ErrTimeout
func (p *Pool) ReleaseTimeout(timeout time.Duration) error {
	if p.IsClosed() || (!p.options.DisablePurge && p.stopPurge == nil) || p.stopTicktock == nil {
		return ErrPoolClosed
	}

	// 清理函数指针，防止内存泄露
	if p.stopPurge != nil {
		p.stopPurge()
		p.stopPurge = nil
	}
	p.stopTicktock()
	p.stopTicktock = nil
	p.Release()

	// 添加超时时间并等待所有任务完成
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

// 重启关闭的pool
func (p *Pool) Reboot() {
	// 使用原子操作开启pool
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		// 启动goroutine
		atomic.StoreInt32(&p.purgeDone, 0)
		p.goPurge()
		atomic.StoreInt32(&p.ticktockDone, 0)
		p.goTicktock()
	}
}

// ---------------------------------------------------------------------------

// 添加running数
func (p *Pool) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

// 添加waiting数
func (p *Pool) addWaiting(delta int) {
	atomic.AddInt32(&p.waiting, int32(delta))
}

// 返回可用的worker来运行task
func (p *Pool) retrieveWorker() (w worker) {
	// 获取cache中worker的匿名函数
	spawnWorker := func() {
		w = p.workerCache.Get().(*goWorker)
		w.run()
	}

	p.lock.Lock()
	w = p.workers.detach()
	if w != nil { // 获取到worker之后解锁
		p.lock.Unlock()
	} else if capacity := p.Cap(); capacity == -1 || capacity > p.Running() { // 当前[]worker容量无限或者有剩余，则获取一个goroutine
		p.lock.Unlock()
		spawnWorker()
	} else { // 没有剩余worker，如果不是Nonblocking模式，则保持阻塞状态，并尝试获取goroutine
		if p.options.Nonblocking {
			p.lock.Unlock()
			return
		}
	retry:
		if p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks {
			p.lock.Unlock()
			return
		}

		// 添加waiting数并保持waiting状态
		p.addWaiting(1)
		p.cond.Wait()
		p.addWaiting(-1)

		// 判断pool状态
		if p.IsClosed() {
			p.lock.Unlock()
			return
		}

		// 尝试获取goroutine
		var nw int
		if nw = p.Running(); nw == 0 {
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

// 放回worker到pool
func (p *Pool) revertWorker(worker *goWorker) bool {
	// 如果没有空间存放worker，返回false
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast()
		return false
	}

	// 获取使用时间
	worker.lastUsed = p.nowTime()

	// 判断pool当前状态
	p.lock.Lock()
	if p.IsClosed() {
		p.lock.Unlock()
		return false
	}

	// 插入worker
	if err := p.workers.insert(worker); err != nil {
		p.lock.Unlock()
		return false
	}

	// 成功返回worker，通知阻塞的worker
	p.cond.Signal()
	p.lock.Unlock()

	return true
}
