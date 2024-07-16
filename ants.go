package ants

import (
	"errors"
	"log"
	"math"
	"os"
	"runtime"
	"time"
)

const (
	// 协程池默认的容量大小
	DefaultAntsPoolSize = math.MaxInt32

	// 清理协程的默认间隔时间
	DefaultCleanIntervalTime = time.Second
)

const (
	//　协程池打开标志位
	OPENED = iota

	//　协程池关闭标志位
	CLOSED
)

var (
	// ErrLackPoolFunc：未提供池的函数
	ErrLackPoolFunc = errors.New("must provide function for pool")

	// ErrInvalidPoolExpiry：设置池清理时间为负数
	ErrInvalidPoolExpiry = errors.New("invalid expiry for pool")

	// ErrPoolClosed：提交task到关闭的池会返回
	ErrPoolClosed = errors.New("this pool has been closed")

	// ErrPoolOverload：当前池没有多余worker可用
	ErrPoolOverload = errors.New("too many goroutines blocked on submit or Nonblocking is set")

	// ErrInvalidPreAllocSize：在PreAlloc模式下，设置负容量
	ErrInvalidPreAllocSize = errors.New("can not set up a negative capacity under PreAlloc mode")

	// ErrTimeout：执行超时
	ErrTimeout = errors.New("operation timed out")

	// workerChanCap确定worker的通道是否应为缓冲通道，以获得最佳性能
	workerChanCap = func() int {
		// 如果GOMAXPROCS=1则使用阻塞channel，将context从sender转换为receiver
		if runtime.GOMAXPROCS(0) == 1 {
			return 0
		}

		// GOMAXPROCS>1则使用非阻塞channel，因为如果sender是CPU密集型，receiver会来不及接收
		return 1
	}()

	// 默认的Logger设置
	defaultLogger = Logger(log.New(os.Stderr, "[ants]: ", log.LstdFlags|log.Lmsgprefix|log.Lmicroseconds))

	// import ants时，初始化一个ants pool
	defaultAntsPool, _ = NewPool(DefaultAntsPoolSize)
)

const nowTimeUpdateInterval = 500 * time.Millisecond

// Logger接口
type Logger interface {
	// 继承log接口
	Printf(format string, args ...interface{})
}

// 提交task到pool
func Submit(task func()) error {
	return defaultAntsPool.Submit(task)
}

// 当前正在运行的协程数
func Running() int {
	return defaultAntsPool.Running()
}

// 协程池的容量
func Cap() int {
	return defaultAntsPool.Cap()
}

// 可用的协程数量
func Free() int {
	return defaultAntsPool.Free()
}

// 关闭协程池
func Release() {
	defaultAntsPool.Release()
}

// 重启协程池
func Reboot() {
	defaultAntsPool.Reboot()
}
