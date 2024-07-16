package ants

import (
	"runtime/debug"
	"time"
)

// task的执行者，创建一个协程接收task并执行函数调用
type goWorkerWithFunc struct {
	// worker pool
	pool *PoolWithFunc

	// 需要被完成的task
	args chan interface{}

	// 放回worker时，更新lastUsed时间
	lastUsed time.Time
}

// 启动goroutine重复执行函数调用
func (w *goWorkerWithFunc) run() {
	// 添加一个running
	w.pool.addRunning(1)
	go func() {
		defer func() {
			// worker执行完成
			w.pool.addRunning(-1)
			// 返回worker到workerCache
			w.pool.workerCache.Put(w)
			// 发生panic，执行设定好的PanicHandler并记录日志
			if p := recover(); p != nil {
				if ph := w.pool.options.PanicHandler; ph != nil {
					ph(p)
				} else {
					w.pool.options.Logger.Printf("worker exits from panic: %v\n%s\n", p, debug.Stack())
				}
			}
			// 释放信号给waiting worker
			w.pool.cond.Signal()
		}()

		// 重复执行task的函数调用
		for args := range w.args {
			if args == nil {
				return
			}
			w.pool.poolFunc(args)
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
		}
	}()
}

// 向task发出空信号
func (w *goWorkerWithFunc) finish() {
	w.args <- nil
}

// 返回lastUsed时间
func (w *goWorkerWithFunc) lastUsedTime() time.Time {
	return w.lastUsed
}

// 输入fn，执行panic
func (w *goWorkerWithFunc) inputFunc(func()) {
	panic("unreachable")
}

// 向args发送arg信号
func (w *goWorkerWithFunc) inputParam(arg interface{}) {
	w.args <- arg
}
