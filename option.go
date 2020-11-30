package mq

const (
	_defaultSize     = 512 * 1 << 10 // 512 Kb
	_defaultDatabase = "mgo_db"
	_defaultCollection    = "mgo_queue"
)

type Options struct {
	Collection  string
	Size        uint64
	Database    string
	Replication bool
}

type Option func(*Options)

func newOption(opts ...Option) Options {
	option := Options{
		Database: 	_defaultDatabase,
		Collection: _defaultCollection,
		Size:     	_defaultSize,
	}

	for _, o := range opts {
		o(&option)
	}

	return option
}

func Collection(name string) Option {
	return func(opt *Options) {
		opt.Collection = name
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
