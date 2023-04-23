package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
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
	// 连接RabbitMQ
	// “amqp://username:password@host:port/virtual_host”
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost))
	failOnError(err, "Failed to connect to RMQConn")
	defer conn.Close()

	// 通过协程创建3个消费者
	for i := 0; i < 3; i++ {
		go Worker(conn, i)
	}

	// 挂起主协程，避免程序退出
	forever := make(chan bool)
	<-forever
}

func Worker(conn *amqp.Connection, workerId int) {
	// 创建一个 rabbitmq 信道, 每个消费者一个
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 声明需要操作的队列
	q, err := ch.QueueDeclare(
		"work_mode", // 队列名
		false,       // 是否需要持久化
		false,       // delete when unused
		false,       // 独占队列只能由声明它们的连接访问，并且连接关闭时将被删除。当前其他连接上的通道尝试声明、绑定、消费、清除或删除同名队列时将反回一个错误。
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 创建一个消费者
	msgChan, err := ch.Consume(
		q.Name, // 需要操作的队列名
		"",     // 消费者唯一id，不填，则自动生成一个唯一值
		true,   // 自动提交消息（即自动确认消息已经处理完成）
		false,  // 当独占为真时，服务器将确保这是这个队列的唯一消费者。当独占为假时，服务器将公平分配跨多个消费者交付。
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// 循环处理消息
	for d := range msgChan {
		log.Printf("[消费者编号=%d] 收到消息: %s", workerId, d.Body)
		// 模拟业务处理，休眠3秒
		time.Sleep(3 * time.Second)
	}
}
