package main

import (
	"fmt"
	"github.com/gzltommy/rbmq"
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
	mqConn, err := rbmq.NewRMQConn(mqUrl)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 3; i++ {
		f := func(id int) {
			consumer, err := rbmq.NewSimpleConsumer(mqConn, "test-queue", false, true)
			if err != nil {
				panic(err)
			}
			err = consumer.Consume(func(payload []byte) error {
				fmt.Printf("--%d---收到消息--------- %s \n", id, string(payload))
				//cancel()
				return nil
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
		mqConn, err := rbmq.NewRMQConn(mqUrl)
		if err != nil {
			panic(err)
		}

		consumer, err := rbmq.NewSubscriptionConsumer(mqConn, "test-pub-sub-exchange", fmt.Sprintf("test-pub-sub-queue-%d", i), false, true)
		if err != nil {
			panic(err)
		}

		f := func(id int) {
			err = consumer.Consume(func(payload []byte) error {
				fmt.Printf("--%d---收到消息--------- %s \n", id, string(payload))
				//cancel()
				return nil
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
	mqConn, err := rbmq.NewRMQConn(mqUrl)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 3; i++ {
		consumer, err := rbmq.NewRoutingConsumer(mqConn, "test-routing-exchange", fmt.Sprintf("test-routing-queue-%d", i+1), fmt.Sprintf("key_%d", i+1), false, true)
		if err != nil {
			panic(err)
		}

		f := func(id int) {
			err = consumer.Consume(func(payload []byte) error {
				fmt.Printf("--%d---收到消息--------- %s \n", id, string(payload))
				//cancel()
				return nil
			})
			if err != nil {
				panic(err)
			}
		}
		id := i + 1
		go f(id)
	}
}
