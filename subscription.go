package rbmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

/*
3 Publish/Subscribe 发布订阅模式，消息被路由投递给多个队列，一个消息被多个消费者获取,生产端不允许指定消费

	应用场景：邮件群发，广告
*/

type SubscriptionPublisher struct {
	mqConn       *RMQConn // 连接
	exchangeName string
}

// NewSubscriptionPublisher
// conn：rabbit mq 连接
// exchangeName：不能为空
// durable：持久化
// autoDelete：自动删除
func NewSubscriptionPublisher(conn *RMQConn, exchangeName string, durable, autoDelete bool) (*SubscriptionPublisher, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if exchangeName == "" {
		return nil, ExchangeNameIsEmpty
	}
	//创建 rabbitmq 实例
	r := &SubscriptionPublisher{
		mqConn:       conn,
		exchangeName: exchangeName,
	}
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 申请交换机,如果交换机不存在则创建,存在则跳过
	err = channel.ExchangeDeclare(
		r.exchangeName,
		"fanout",   // 交换机类型，fanout 发布订阅模式
		durable,    // 是否持久化
		autoDelete, // 自动删除
		false,
		false,
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
func (r *SubscriptionPublisher) Publish(message []byte, expirationSecond uint64) (err error) {
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	expiration := ""
	if expirationSecond > 0 {
		expiration = fmt.Sprintf("%d", expirationSecond*1000)
	}
	// 发送消息
	err = channel.Publish(
		r.exchangeName,
		"", // key 路由参数，fanout 类型交换机，自动忽略路由参数，填了也没用。
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

type SubscriptionConsumer struct {
	*baseConsumer
}

// NewSubscriptionConsumer 获取订阅模式下的 rabbitmq 的实例
// conn：rabbit mq 连接
// exchangeName：必填参数
// queueName：为空时，自动生成
// consumer：为空时，自动生成
// durable：是否需要持久化
// autoDelete：是否需要自动删除
func NewSubscriptionConsumer(conn *RMQConn, exchangeName, queueName, consumer string, durable, autoDelete bool) (IConsumer, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if exchangeName == "" {
		return nil, ExchangeNameIsEmpty
	}
	r := &SubscriptionConsumer{}
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

	// 1、申请交换机,如果交换机不存在则创建,存在则跳过
	err = channel.ExchangeDeclare(
		exchangeName,
		"fanout",   // 交换机类型，fanout 发布订阅模式
		durable,    // 是否持久化
		autoDelete, // 自动删除
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//2、申请队列,如果队列不存在则创建,存在则跳过
	q, err := channel.QueueDeclare(
		queueName,  // 队列名字，不填则随机生成一个
		durable,    // 是否持久化队列
		autoDelete, // 自动删除
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}
	r.queueName = q.Name

	//3、绑定队列到交换机中
	err = channel.QueueBind(
		r.queueName,  // 队列名
		"",           // key 路由参数，fanout 类型交换机，自动忽略路由参数
		exchangeName, // 交换机名字，需要跟消息发送端定义的交换器保持一致
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}
