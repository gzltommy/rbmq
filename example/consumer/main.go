package main

import (
	"context"
	"fmt"
	"github.com/gzltommy/rbmq/rabbitmq"
	"time"
)

const (
	UserName    = "guest"
	Password    = "guest"
	Host        = "192.168.200.133"
	Port        = "5672"
	VirtualHost = "scan"
)

func main() {
	//Simple()
	//PubSub()
	Routing()
	time.Sleep(time.Hour * 2)
}
func Simple() {
	mqUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost)
	mqConn, err := rabbitmq.NewRMQConn(mqUrl)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 3; i++ {
		f := func(id int) {
			consumer, err := rabbitmq.NewSimpleConsumer(mqConn, "test-queue", fmt.Sprintf("Simple%d", id), false, true)
			if err != nil {
				panic(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			_ = cancel
			err = consumer.Consume(ctx, func(payload []byte) bool {
				fmt.Printf("--%d---收到消息--------- %s \n", id, string(payload))
				//cancel()
				return true
			})
			if err != nil {
				panic(err)
			}

		}
		id := i + 1
		go f(id)
	}
}

func PubSub() {
	for i := 0; i < 3; i++ {
		mqUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost)
		mqConn, err := rabbitmq.NewRMQConn(mqUrl)
		if err != nil {
			panic(err)
		}

		consumer, err := rabbitmq.NewSubscriptionConsumer(mqConn, "test-pub-sub-exchange", fmt.Sprintf("test-pub-sub-queue-%d", i), "", false, true)
		if err != nil {
			panic(err)
		}

		f := func(id int) {
			ctx, _ := context.WithCancel(context.Background())
			err = consumer.Consume(ctx, func(payload []byte) bool {
				fmt.Printf("--%d---收到消息--------- %s \n", id, string(payload))
				//cancel()
				return true
			})
			if err != nil {
				panic(err)
			}
		}
		id := i + 1
		go f(id)
	}
}

func Routing() {
	mqUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost)
	mqConn, err := rabbitmq.NewRMQConn(mqUrl)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 3; i++ {
		consumer, err := rabbitmq.NewRoutingConsumer(mqConn, "test-routing-exchange", fmt.Sprintf("test-routing-queue-%d", i+1), fmt.Sprintf("key_%d", i+1), "", false, true)
		if err != nil {
			panic(err)
		}

		f := func(id int) {
			ctx, _ := context.WithCancel(context.Background())
			err = consumer.Consume(ctx, func(payload []byte) bool {
				fmt.Printf("--%d---收到消息--------- %s \n", id, string(payload))
				//cancel()
				return true
			})
			if err != nil {
				panic(err)
			}
		}
		id := i + 1
		go f(id)
	}
}
