package rbmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

/*
4 Routing 路由模式,一个消息被多个消费者获取，并且消息的目标队列可以被生产者指定

	应用场景: 根据生产者的要求发送给特定的一个或者一批队列发送信息
*/

type RoutingPublisher struct {
	mqConn       *RMQConn // 连接
	exchangeName string
}

// NewRoutingPublisher
// conn：rabbit mq 连接
// exchangeName：不能为空
// queueName：可为空，为空则自动生成
// routingKey：绑定路由
// consumer：消费者名
// durable：持久化
// autoDelete：自动删除
func NewRoutingPublisher(conn *RMQConn, exchangeName string, durable, autoDelete bool) (*RoutingPublisher, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if exchangeName == "" {
		return nil, ExchangeNameIsEmpty
	}

	r := &RoutingPublisher{
		mqConn:       conn,
		exchangeName: exchangeName,
	}
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 尝试创建交换机，不存在创建
	err = channel.ExchangeDeclare(
		r.exchangeName, //交换机名称
		"direct",       //交换机类型 广播类型
		durable,        //是否持久化
		autoDelete,     //是否字段删除
		false,          //true 表示这个 exchange 不可以被 client 用来推送消息，仅用来进行 exchange 和 exchange 之间的绑定
		false,          //是否阻塞 true 表示要等待服务器的响应
		nil,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Publish
// message：消息内容
// expirationSecond：过期时间，秒；0 表示永不过期
func (r *RoutingPublisher) Publish(message []byte, routingKey string, expirationSecond uint64) (err error) {
	if len(routingKey) == 0 {
		return RoutingKeyIsRequired
	}
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	expiration := ""
	if expirationSecond > 0 {
		expiration = fmt.Sprintf("%d", expirationSecond*1000)
	}
	err = channel.Publish(
		r.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Expiration:   expiration,      // 过期毫秒数
			DeliveryMode: amqp.Persistent, // 持久化
			ContentType:  "text/plain",
			Body:         message,
			Timestamp:    time.Now(),
		})
	if err != nil {
		return err
	}
	return nil
}

type RoutingConsumer struct {
	*baseConsumer
}

// NewRoutingConsumer
// conn：rabbit mq 连接
// exchangeName：不能为空
// queueName：可为空，为空则自动生成
// routingKey：绑定路由,不能为空
// consumer：消费者名,可为空
// durable：持久化
// autoDelete：自动删除
func NewRoutingConsumer(conn *RMQConn, exchangeName, queueName, routingKey, consumer string, durable, autoDelete bool) (IConsumer, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if exchangeName == "" {
		return nil, ExchangeNameIsEmpty
	}

	if len(routingKey) == 0 {
		return nil, RoutingKeyIsRequired
	}
	r := &RoutingConsumer{}
	r.baseConsumer = &baseConsumer{
		mqConn:        conn,
		prefetchCount: DefaultPrefetchCount,
		iC:            r,
		consumer:      consumer,
	}
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 1、尝试创建交换机，不存在创建
	err = channel.ExchangeDeclare(
		exchangeName, //交换机名称
		"direct",     //交换机类型 广播类型
		durable,      //是否持久化
		autoDelete,   //是否字段删除
		false,        //true 表示这个 exchange 不可以被 client 用来推送消息，仅用来进行 exchange 和 exchange 之间的绑定
		false,        //是否阻塞 true 表示要等待服务器的响应
		nil,
	)

	if err != nil {
		return nil, err
	}

	//2、 试探性创建队列
	q, err := channel.QueueDeclare(
		queueName,  // 队列名字
		durable,    // 是否持久化队列
		autoDelete, // 自动删除
		false,      // exclusive 独占队列只能由声明它们的连接访问，并且连接关闭时将被删除。当前其他连接上的通道尝试声明、绑定、消费、清除或删除同名队列时将反回一个错误。
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	r.queueName = q.Name

	//3、绑定队列到 exchange中
	err = channel.QueueBind(
		r.queueName,
		routingKey, // 绑定关系中的 key
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return r, nil
}
