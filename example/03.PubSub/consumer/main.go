package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
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
	// 1、连接rabbitmq
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
	// 2、创建信道，通常一个消费者一个
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 3、声明 fanout 交换机
	err = ch.ExchangeDeclare(
		"fanout_test", // 交换机名，需要跟消息发送方保持一致
		"fanout",      // 交换机类型
		true,          // 是否持久化,进入队列如果不消费那么消息就在队列里面,如果重启服务器那么这个消息就没啦 通常设置为 false
		false,         // auto-deleted 是否为自动删除  这里解释的会更加清楚：https://blog.csdn.net/weixin_30646315/article/details/96224842?utm_medium=distribute.pc_relevant_t0.none-task-blog-BlogCommendFromMachineLearnPai2-1.nonecase&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-BlogCommendFromMachineLearnPai2-1.nonecase
		false,         // internal true 表示这个 exchange 不可以被客户端用来推送消息，仅仅是用来进行 exchange 和 exchange 之间的绑定
		false,         // no-wait 直接 false 即可 也不知道干啥滴
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 4、声明需要操作的队列
	q, err := ch.QueueDeclare(
		"",    // 队列名字，不填则随机生成一个
		false, // 是否持久化队列
		false, // delete when unused
		true,  // 独占队列只能由声明它们的连接访问，并且连接关闭时将被删除。当前其他连接上的通道尝试声明、绑定、消费、清除或删除同名队列时将反回一个错误。
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//5、队列绑定指定的交换机
	err = ch.QueueBind(
		q.Name,        // 队列名
		"",            // 路由参数，fanout 类型交换机，自动忽略路由参数
		"fanout_test", // 交换机名字，需要跟消息发送端定义的交换器保持一致
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// 6、创建消费者
	msgs, err := ch.Consume(
		q.Name, // 引用前面的队列名
		"",     // 消费者名字，不填自动生成一个
		true,   // 自动向队列确认消息已经处理
		false,  // 当独占为真时，服务器将确保这是这个队列的唯一消费者。当独占为假时，服务器将公平分配跨多个消费者交付。
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// 循环消费队列中的消息
	for d := range msgs {
		log.Printf("[消费者编号=%d] 接收消息:%s", workerId, d.Body)
	}
}
