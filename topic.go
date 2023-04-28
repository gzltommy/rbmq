package rbmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

/*
5 Topic 话题模式,一个消息被多个消息获取，消息的目标 queue 可用 BindKey 以通配符

	（#:一个或多个词，*：一个词）的方式指定。
*/

type TopicPublisher struct {
	mqConn       *RMQConn // 连接
	exchangeName string
}

// NewTopicPublisher 创建 Topic 模式下的 publisher
// conn：rabbit mq 连接
// exchangeName：不能为空
// durable：持久化
// autoDelete：自动删除
func NewTopicPublisher(conn *RMQConn, exchangeName string, durable, autoDelete bool) (*TopicPublisher, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if exchangeName == "" {
		return nil, ExchangeNameIsEmpty
	}
	r := &TopicPublisher{
		mqConn:       conn,
		exchangeName: exchangeName,
	}

	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 尝试创建交换机,这里的 kind 的类型要改为 topic
	err = channel.ExchangeDeclare(
		exchangeName,
		"topic",
		durable,
		autoDelete,
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
func (r *TopicPublisher) Publish(message []byte, routingKey string, expirationSecond uint64) (err error) {
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
	// 发送消息。
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

type TopicConsumer struct {
	*BaseConsumer
}

// NewTopicConsumer 创建 Topic 模式下的 consumer
// conn：rabbit mq 连接
// exchangeName:必填参数，不能为空
// queueName:可为空，为空则自动生成，队列名为空时，队列强制为非持久化和自动删除
// routingKey:不能为空
// durable：持久化
// autoDelete：自动删除
func NewTopicConsumer(conn *RMQConn, exchangeName, queueName, routingKey string, durable, autoDelete bool) (IConsumer, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if exchangeName == "" {
		return nil, ExchangeNameIsEmpty
	}

	if len(routingKey) == 0 {
		return nil, RoutingKeyIsRequired
	}

	channel, err := conn.GetConn().Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 尝试创建交换机,这里的 kind 的类型要改为 topic
	err = channel.ExchangeDeclare(
		exchangeName,
		"topic",
		durable,
		autoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// 队列名为空时，队列强制为非持久化和自动删除
	if queueName == "" {
		durable = false
		autoDelete = true
	}

	//2 尝试创建队列，存在自动跳过
	q, err := channel.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	//2 将队列绑定到交换机里。
	err = channel.QueueBind(
		q.Name,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	c := &TopicConsumer{}
	c.BaseConsumer = NewBaseConsumer(conn, DefaultPrefetchCount, q.Name, c)

	return c, nil
}
