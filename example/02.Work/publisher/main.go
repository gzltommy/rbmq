package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

// 错误处理
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

// PS：该实例要先启动消费者程序，再启动生产者程序，不然观察不到效果
// 如果先启动生产者程序，再启动消费者程序，那么启动的消费者程序中的第一个消费者会把滞留在队列中的消息全部取走
func main() {
	// 1、连接RabbitMQ
	// “amqp://username:password@host:port/virtual_host”
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost))
	failOnError(err, "Failed to connect to RMQConn")
	defer conn.Close()

	// 2、创建信道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//3、声明要操作的队列
	q, err := ch.QueueDeclare(
		"work_mode", // name
		false,       // durable
		false,       // delete when unused
		false,       // 独占队列只能由声明它们的连接访问，并且连接关闭时将被删除。当前其他连接上的通道尝试声明、绑定、消费、清除或删除同名队列时将反回一个错误。
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 4、循环发送消息
	for i := 0; i < 10; i++ {
		body := fmt.Sprintf("Hello World! ---- %d", i) // 要发送的消息内容
		err = ch.Publish(
			"",     // exchange 交换机 simple 模式下默认为空，虽然为空，但其实也是在用的 rabbitmq 当中的 default 交换机运行
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")
		log.Printf(" [x] Sent %s", body)
	}
}
