package mq

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	pkgerr "github.com/pkg/errors"
)

const (
	namespaceExistsErrCode int32 = 48
)

type Channel struct {
	opts Options

	client *mongo.Database
	ctx    context.Context
}

type Message struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Content   interface{}
	CreatedAt int64
	Offset    bool
}

func NewChannel(ctx context.Context, opts ...Option) (*Channel, error) {
	opt := newOption(opts...)
	if opt.MongoURI == "" {
		return nil, pkgerr.New("mongo uri could not empty")
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(opt.MongoURI))
	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	c := &Channel{
		opts:   opt,
		ctx:    ctx,
		client: client.Database(opt.Database),
	}

	if err := c.exec(&opt); err != nil {
		return nil, err
	}

	return c, nil
}

// create database and collection
func (c *Channel) exec(opt *Options) error {
	cmd := bson.D{
		{Key: "create", Value: opt.Topic},
		{Key: "capped", Value: true},
		{Key: "size", Value: opt.Size},
	}

	if !opt.Replication {
		cmd = append(cmd, bson.E{Key: "autoIndexId", Value: false})
	}

	if err := c.client.RunCommand(c.ctx, cmd).Err(); err != nil {
		// ignore NamespaceExists errors for idempotency

		cmdErr, ok := err.(mongo.CommandError)
		if !ok || cmdErr.Code != namespaceExistsErrCode {
			log.Println(fmt.Sprintf("creating collection %v on server: %v failed", opt.Topic, err))
			return err
		}

		log.Println(fmt.Sprintf("topic name(%s.%s) already exist", opt.Database, opt.Topic))
	}

	return nil
}

func (c *Channel) Publish(topic string, v interface{}) {
	msg := &Message{Content: v, CreatedAt: time.Now().Unix()}
	_, err := c.client.Collection(topic).InsertOne(c.ctx, msg)
	if err != nil {
		log.Println("channel insert document to collection failed", err)
	}
	return
}

func (c *Channel) Subscribe(topic string) (<-chan Message, error) {
	col := c.client.Collection(topic)
	opt := options.Find().SetCursorType(options.TailableAwait).SetNoCursorTimeout(true)
	rs, err := col.Find(c.ctx, bson.D{{Key: "offset", Value: false}}, opt)
	if err != nil {
		return nil, err
	}

	msg := Message{}
	ch := make(chan Message)

	go func() {
		for {
			if rs.Err() != nil || rs.ID() == 0 {
				close(ch)
				return
			}

			if !rs.TryNext(c.ctx) {
				<-time.After(1 * time.Second)
				continue
			}

			if err := rs.Decode(&msg); err != nil {
				log.Println("decode error", err)
				continue
			}

			select {
			case ch <- msg:
				err := c.markOffset(topic, msg.ID)
				if err != nil {
					log.Println("mark offset failed", err)
				}
			}

		}
	}()

	return ch, nil
}

func (c *Channel) markOffset(topic string, id primitive.ObjectID) error {
	filter := bson.D{{Key: "_id", Value: id}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "offset", Value: true}}}}
	_, err := c.client.Collection(topic).UpdateOne(c.ctx, filter, update)
	return err
}
