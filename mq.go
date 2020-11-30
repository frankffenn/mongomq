package mq

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/xerrors"
)

const (
	namespaceExistsErrCode int32 = 48
)

var _client *mongo.Client

type Channel struct {
	opts    Options
	ctx     context.Context
	handler chan string
	db      *mongo.Database
	sync.RWMutex
	coll    *mongo.Collection
	buses  map[string]*runner
}

type runner struct {
	dbname string
	name   string
	topic  string
	start  bool

	closed   chan struct{}
	msgQueue chan Message
}

type Message struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Topic     string
	Content   interface{}
	CreatedAt int64
	Offset    bool
}

func init() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("empty mongo uri")
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("create mongo client failed", err)
	}

	if err = client.Connect(context.TODO()); err != nil {
		log.Fatal("connecting error:",err)
	}

	_client = client
}

// NewChannel create the pub/sub Channel
func NewChannel(ctx context.Context, opts ...Option) (*Channel, error) {
	opt := newOption(opts...)

	c := &Channel{
		opts:    opt,
		ctx:     ctx,
		db:      _client.Database(opt.Database),
		coll:    _client.Database(opt.Database).Collection(opt.Collection),
		handler: make(chan string),
		buses : map[string]*runner{},
	}

	if err := c.createCollection(); err != nil {
		return nil, err
	}

	go c.start()

	return c, nil
}

func (c *Channel) createCollection() error {
	cmd := bson.D{
		{Key: "create", Value: c.opts.Collection},
		{Key: "capped", Value: true},
		{Key: "size", Value: c.opts.Size},
	}

	if c.opts.Replication {
		cmd = append(cmd, bson.E{Key: "autoIndexId", Value: true})
	}

	if err := c.db.RunCommand(c.ctx, cmd).Err(); err != nil {
		// ignore NamespaceExists errors for idempotence

		cmdErr, ok := err.(mongo.CommandError)
		if !ok || cmdErr.Code != namespaceExistsErrCode {
			return err
		}
	}

	return nil
}

// Publish publish the giving message to all subscribers of the specified topics
func (c *Channel) Publish(topic string, data interface{}) error {
	c.Lock()
	defer c.Unlock()

	r, ok := c.buses[topic]
	if !ok {
		r = &runner{
			name:   c.opts.Collection,
			dbname: c.opts.Database,
			topic:  topic,
			start:  true,

			msgQueue: make(chan Message),
			closed:   make(chan struct{}),
		}
		c.buses[topic] = r
		go r.run()
	}

	msg := &Message{Content: data, Topic: topic, CreatedAt: time.Now().Unix()}
	_, err := c.coll.InsertOne(c.ctx, msg)
	if err != nil {
		return xerrors.Errorf("channel insert document to collection failed: %w", err)
	}

	return nil
}

// Subscribe return a channel which receives message from specified topics.
func (c *Channel) Subscribe(topic string) (<-chan Message, error) {

	for {
		time.Sleep(3 * time.Second)

		c.Lock()
		runner, ok := c.buses[topic]
		c.Unlock()
		if ok && runner != nil {
			return runner.msgQueue, nil
		}
	}
}

// Ack acknowledge the message has been consumed.
func (c *Channel) Ack(topic string, id primitive.ObjectID) error {
	filter := bson.D{{Key: "_id", Value: id}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "offset", Value: true}}}}
	_, err := c.db.Collection(topic).UpdateOne(c.ctx, filter, update)
	return err
}

// UnSub unsubscribes the given channel from the specified topics.
func (c *Channel) UnSub(topic string) {
	c.Lock()
	defer c.Unlock()

	runner, ok := c.buses[topic]
	if !ok {
		return
	}

	runner.closed <- struct{}{}
}

func (c *Channel) start() {
	c.Lock()
	defer c.Unlock()

	for _, runner := range c.buses {
		if runner.start {
			continue
		}
		go runner.run()
	}
}

func (r *runner) run() {
	ctx := context.Background()
	col := _client.Database(r.dbname).Collection(r.name)
	opt := options.Find().SetCursorType(options.TailableAwait).SetNoCursorTimeout(true)
	rs, err := col.Find(ctx, bson.D{{Key:"topic",Value: r.topic},{Key: "offset", Value: false}}, opt)
	if err != nil {
		log.Printf("query from mongo failed,%v\n", err)
		return
	}

	if rs.Err() != nil || rs.ID() == 0 {
		log.Printf("rs.err%v,%d\n", rs.Err(), rs.ID())
	}

	defer rs.Close(ctx)

	var msg Message

	for {
		select {
		case <-r.closed:
			return
		default:
		}

		if !rs.TryNext(ctx) {
			<-time.After(1 * time.Second)
			continue
		}

		if err := rs.Decode(&msg); err != nil {
			continue
		}

		r.msgQueue <- msg

		r.ack(msg.ID)
	}
}

func (r *runner) ack(id primitive.ObjectID) error {
	ctx := context.Background()
	filter := bson.D{{Key: "_id", Value: id}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "offset", Value: true}}}}
	_, err := _client.Database(r.dbname).Collection(r.name).UpdateOne(ctx, filter, update)
	return err
}
