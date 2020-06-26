package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mq "github.com/frankffenn/mongomq"
)

const (
	TestURI   = "mongodb://root:example@127.0.0.1:7017,127.0.0.1:7018,127.0.0.1:7019/test?authsource=admin"
	TestTopic = "col1"
	TestDB    = "test"
)

func main() {
	ctx := context.Background()

	mq, err := mq.NewChannel(
		ctx,
		mq.Topic(TestTopic),
		mq.Database(TestDB),
		mq.MongoURI(TestURI),
		mq.Replication(true),
	)
	if err != nil {
		log.Fatalf("create channel failed, %v", err)
	}

	go func() {
		for i := 0; i < 5; i++ {
			mq.Publish(TestTopic, fmt.Sprintf("hello mq , index=%d", i))
			<-time.After(1 * time.Second)
		}
	}()

	msg, err := mq.Subscribe(TestTopic)
	if err != nil {
		log.Fatalf("subscribe channel failed, %v", err)
	}

	for {
		select {
		case m, ok := <-msg:
			if ok {
				fmt.Println("got msg from mongo mq", m.Content, m.CreatedAt)
			}
		}
	}
}