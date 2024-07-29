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

	///2、创建信道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//3、声明 fanout 交换机
	err = ch.ExchangeDeclare(
		"fanout_test", // 交换机名字
		"fanout",      // 交换机类型，fanout 发布订阅模式
		true,          // 是否持久化,进入队列如果不消费那么消息就在队列里面,如果重启服务器那么这个消息就没啦 通常设置为 false
		false,         // auto-deleted 是否为自动删除  这里解释的会更加清楚：https://blog.csdn.net/weixin_30646315/article/details/96224842?utm_medium=distribute.pc_relevant_t0.none-task-blog-BlogCommendFromMachineLearnPai2-1.nonecase&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-BlogCommendFromMachineLearnPai2-1.nonecase
		false,         // internal true 表示这个 exchange 不可以被客户端用来推送消息，仅仅是用来进行 exchange 和 exchange 之间的绑定
		false,         // no-wait 直接 false 即可 也不知道干啥滴
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 4、推送消息
	for i := 0; i < 6; i++ {
		body := fmt.Sprintf("Hello! -- %d", i) // 消息内容
		err = ch.Publish(
			"fanout_test", // exchange（交换机名字，跟前面声明对应）
			"",            // 路由参数，fanout 类型交换机，自动忽略路由参数，填了也没用。
			false,         // mandatory
			false,         // immediate
			amqp.Publishing{
				ContentType: "text/plain", // 消息内容类型，这里是普通文本
				Body:        []byte(body), // 消息内容
			})
		log.Printf("发送内容 %s", body)
	}
}
