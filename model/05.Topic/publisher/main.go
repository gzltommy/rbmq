package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

const (
	UserName    = "guest"
	Password    = "guest"
	Host        = "127.0.0.1"
	Port        = "5672"
	VirtualHost = "/"
)

func main() {
	// 1、连接 rabbitmq
	// “amqp://username:password@host:port/virtual_host”
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost))
	failOnError(err, "Failed to connect to RMQConn")
	defer conn.Close()

	//2、创建信道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//3、声明 topic 交换机
	err = ch.ExchangeDeclare(
		"topic_test", // 交换机名字
		"topic",      // 交换机类型，topic
		true,         // 是否持久化
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 4、推送消息
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf("Hello! -- %d", i) // 消息内容
		err = ch.Publish(
			"topic_test",             // exchange（交换机名字，跟前面声明对应）
			fmt.Sprintf("a.b.%d", i), // 路由参数，关键参数，决定你的消息会发送到那个队列。
			false,                    // mandatory
			false,                    // immediate
			amqp.Publishing{
				ContentType: "text/plain", // 消息内容类型，这里是普通文本
				Body:        []byte(body), // 消息内容
			})
		log.Printf("发送内容 %s", body)
	}
}
