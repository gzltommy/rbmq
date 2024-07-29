package main

// 导入包
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
	// 1、连接 RMQConn
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
		"hello_queue", // 队列名需要跟发送消息的队列名保持一致
		false,         // durable
		false,         // delete when unused
		false,         // 独占队列只能由声明它们的连接访问，并且连接关闭时将被删除。当前其他连接上的通道尝试声明、绑定、消费、清除或删除同名队列时将反回一个错误。
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//PS: simple 模式下交换机为空因为会默认使用 rabbitmq 默认的 default 交换机而不是真的不需要交换机

	// 创建消息消费者
	msgChan, err := ch.Consume(
		//q.Name, // 队列名
		"hello_queue", // 队列名
		"",            // 消费者名字，不填，则自动生成一个唯一ID
		true,          // 是否自动提交消息，即自动告诉 rabbitmq 消息已经处理成功你可以去删除这个消息啦 默认是 true。
		false,         // 当独占为真时，服务器将确保这是这个队列的唯一消费者。当独占为假时，服务器将公平分配跨多个消费者交付。
		false,         // no-local 如果设置为 true 表示不能将同一个 connection 中发送的消息传递给同个 connection 中的消费者
		false,         // no-wait 队列消费是否阻塞 false 表示是阻塞 true 表示是不阻塞
		nil,           // args
	)
	failOnError(err, "Failed to register a consumer")

	// 循环拉取队列中的消息
	for d := range msgChan {
		// 打印消息内容
		log.Printf("Received a message: %s", d.Body)
	}
}
