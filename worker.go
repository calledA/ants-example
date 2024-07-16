package ants

import (
	"runtime/debug"
	"time"
)

// task的执行者，创建一个协程接收task并执行函数调用
type goWorker struct {
	// worker pool
	pool *Pool

	// 需要被完成的task
	task chan func()

	// 放回worker时，更新lastUsed时间
	lastUsed time.Time
}

// 启动goroutine重复执行函数调用
func (w *goWorker) run() {
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
		for f := range w.task {
			if f == nil {
				return
			}
			f()
			// TODO
			if ok := w.pool.revertWorker(w); !ok {
				return
			}
		}
	}()
}

// 向task发出空信号
func (w *goWorker) finish() {
	w.task <- nil
}

// 返回lastUsed时间
func (w *goWorker) lastUsedTime() time.Time {
	return w.lastUsed
}

// 向task发送fn信号
func (w *goWorker) inputFunc(fn func()) {
	w.task <- fn
}

// 输入参数，执行panic
func (w *goWorker) inputParam(interface{}) {
	panic("unreachable")
}
