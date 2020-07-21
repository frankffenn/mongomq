# mongomq

mongomq is a pub/sub implementation for golang and MongoDB.

## Install

`
    go get -v github.com/frankffenn/mongomq
`

## Example 

```
package main

import (
	"context"
	"log"

	mq "github.com/frankffenn/mongomq"
)

func main() {
	ctx := context.Background()
	c, err := mq.NewChannel(ctx,
		mq.Database("testdb"),
		mq.Topic("testtopic"),
	)

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for i := 0; i < 10; i++ {
			c.Publish("testtopic", "hi")
		}
	}()

	data, err := c.Subscribe("testtopic")
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case msg := <-data:
			println(msg)
		}
	}

}
```

View the [MongoMQ Example](https://github.com/frankffenn/mongomq/blob/master/example/example_mq.go)

## Status 

Just for testing. This isn't used in production yet.