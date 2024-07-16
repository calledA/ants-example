package ants

import (
	"errors"
	"time"
)

var (
	// queue满了时
	errQueueIsFull = errors.New("the queue is full")

	// 尝试插入release的queue
	errQueueIsReleased = errors.New("the queue length is zero")
)

// worker接口
type worker interface {
	run()
	finish()
	lastUsedTime() time.Time
	inputFunc(func())
	inputParam(interface{})
}

// worker的接口
type workerQueue interface {
	// 当前worker长度
	len() int
	// 判断worker是否为空
	isEmpty() bool
	// 插入worker数据
	insert(worker) error
	// 返回worker最后一位的数据
	detach() worker
	// 清理过期的worker
	refresh(duration time.Duration) []worker
	// 二分查找过期的worker
	reset()
}

type queueType int

const (
	queueTypeStack queueType = 1 << iota
	queueTypeLoopQueue
)

// 根据qType初始化worker的类型
func newWorkerArray(qType queueType, size int) workerQueue {
	switch qType {
	case queueTypeStack:
		return newWorkerStack(size)
	case queueTypeLoopQueue:
		return newWorkerLoopQueue(size)
	default:
		return newWorkerStack(size)
	}
}
