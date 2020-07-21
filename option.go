package mq

const (
	_defaultSize     = 512 * 1 << 10 // 512 Kb
	_defaultDatabase = "mqdatabase"
	_defaultTopic    = "mqtopic"
)

type Options struct {
	Topic       string
	Size        uint64
	Database    string
	Replication bool
}

type Option func(*Options)

func newOption(opts ...Option) Options {
	option := Options{
		Database: _defaultDatabase,
		Topic:    _defaultTopic,
		Size:     _defaultSize,
	}

	for _, o := range opts {
		o(&option)
	}

	return option
}

func Topic(name string) Option {
	return func(opt *Options) {
		opt.Topic = name
	}
}

func Size(size uint64) Option {
	return func(opt *Options) {
		opt.Size = size
	}
}

func Database(name string) Option {
	return func(opt *Options) {
		opt.Database = name
	}
}

func Replication(b bool) Option {
	return func(opt *Options) {
		opt.Replication = b
	}
}
