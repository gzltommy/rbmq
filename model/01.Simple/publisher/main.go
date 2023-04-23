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
	Host        = "192.168.24.133"
	Port        = "5672"
	VirtualHost = "/"
)

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

	//3、声明要操作的队列,如果队列不存在，则会自动创建，如果队列存在则跳过创建直接使用  这样的好处保障队列存在，消息能发送到队列当中
	_, err = ch.QueueDeclare(
		"hello_queue", // name
		false,         // durable 消息是否持久化：进入队列如果不消费那么消息就在队列里面 如果重启服务器那么这个消息就没啦 通常设置为false
		false,         // autoDelete 是否为自动删除：意思是最后一个消费者断开链接以后是否将消息从队列当中删除  默认设置为false不自动删除
		false,         // 独占队列只能由声明它们的连接访问，并且连接关闭时将被删除。当前其他连接上的通道尝试声明、绑定、消费、清除或删除同名队列时将反回一个错误。
		false,         // no-wait 是否阻塞：发送消息以后是否要等待消费者的响应 消费了下一个才进来 就跟 golang 里面的无缓冲 channle 一个道理 默认为非阻塞即可设置为 false
		nil,           // arguments 其他的属性，没有则直接诶传入空即可 nil
	)
	failOnError(err, "Failed to declare a queue")

	// 4、发送消息
	body := "Hello World!" // 要发送的消息内容
	err = ch.Publish(
		"", // exchange 交换机 simple 模式下默认为空，虽然为空，但其实也是在用的 rabbitmq 当中的 default 交换机运行
		//q.Name, // routing key 在 simple 模式下，将路由 Key 设置为队列的名称
		"hello_queue", // routing key 在 simple 模式下，将路由 Key 设置为队列的名称
		false,         // mandatory 强制性的，如果为 true 会根据 exchange 类型和 routeKey 规则，如果无法找到符合条件的队列那么会把发送的消息返还给发送者
		false,         // immediate 立刻反应，如果为 true，当 exchange 发送消息到队列后发现队列上没有绑定消费者则会把消息返还给发送者
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
