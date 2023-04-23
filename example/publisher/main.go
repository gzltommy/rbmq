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
	mqUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost)
	mqConn, err := rbmq.NewRMQConn(mqUrl)
	if err != nil {
		panic(err)
	}

	//Simple(mqConn)
	//PubSub(mqConn)
	Routing(mqConn)

	time.Sleep(time.Hour * 2)
}

func Simple(mqConn *rbmq.RMQConn) {
	publisher, err := rbmq.NewSimplePublisher(mqConn, "test-queue", false, true)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 5; i++ {
		err = publisher.Publish([]byte(fmt.Sprintf("测试消息%d", i+1)), 6)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second * 1)
	}
}

func PubSub(mqConn *rbmq.RMQConn) {
	publisher, err := rbmq.NewSubscriptionPublisher(mqConn, "test-pub-sub-exchange", false, true)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 110; i++ {
		err = publisher.Publish([]byte(fmt.Sprintf("广播消息：%d", i+1)), 0)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}

func Routing(mqConn *rbmq.RMQConn) {
	publisher, err := rbmq.NewRoutingPublisher(mqConn, "test-routing-exchange", false, true)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 3; i++ {
		err = publisher.Publish([]byte(fmt.Sprintf("消息：%d", i+1)), fmt.Sprintf("key_%d", i+1), 0)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
}
