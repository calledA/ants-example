package ants

import "time"

// 定义option方法
type Option func(opts *Options)

// 创建option实例
func loadOptions(options ...Option) *Options {
	opts := new(Options)
	for _, option := range options {
		option(opts)
	}
	return opts
}

// 初始化ants pool时的option参数
type Options struct {
	// 每隔ExpiryDuration时间扫描所有worker，清理未超过ExpiryDuration的worker
	ExpiryDuration time.Duration

	// 初始化ants pool时的预分配容量
	PreAlloc bool

	// pool.Submit最大的goroutine阻塞数量，0表示没有限制
	MaxBlockingTasks int

	// Nonblocking=true时，pool.Submit不会被阻塞且MaxBlockingTasks将会不起作用。
	// pool.Submit不能一次完成时返回ErrPoolOverload
	Nonblocking bool

	// PanicHandler用于处理worker中的panic，如果为nil，则panic将从goroutine中再次抛出
	PanicHandler func(interface{})

	// 自定义日志记录器，如果未设置，则使用日志包中的默认标准记录器
	Logger Logger

	// 当DisablePurge=true时，worker不会被清除
	DisablePurge bool
}

// 通过option模式设置options config
func WithOptions(options Options) Option {
	return func(opts *Options) {
		*opts = options
	}
}

// 通过option模式设置ExpiryDuration
func WithExpiryDuration(expiryDuration time.Duration) Option {
	return func(opts *Options) {
		opts.ExpiryDuration = expiryDuration
	}
}

// 通过option模式设置PreAlloc
func WithPreAlloc(preAlloc bool) Option {
	return func(opts *Options) {
		opts.PreAlloc = preAlloc
	}
}

// 通过option模式设置MaxBlockingTasks
func WithMaxBlockingTasks(maxBlockingTasks int) Option {
	return func(opts *Options) {
		opts.MaxBlockingTasks = maxBlockingTasks
	}
}

// 通过option模式设置Nonblocking
func WithNonblocking(nonblocking bool) Option {
	return func(opts *Options) {
		opts.Nonblocking = nonblocking
	}
}

// 通过option模式设置PanicHandler
func WithPanicHandler(panicHandler func(interface{})) Option {
	return func(opts *Options) {
		opts.PanicHandler = panicHandler
	}
}

// 通过option模式设置Logger
func WithLogger(logger Logger) Option {
	return func(opts *Options) {
		opts.Logger = logger
	}
}

// 通过option模式设置DisablePurge
func WithDisablePurge(disable bool) Option {
	return func(opts *Options) {
		opts.DisablePurge = disable
	}
}
