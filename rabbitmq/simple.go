package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

/*
1 Simple 模式，最简单最常用的模式，一个消息只能被一个消费者消费

	应用场景: 短信，聊天

2 Work 模式，一个消息只能被一个消费者消费

	应用场景: 抢红包，和资源任务调度
*/

type SimplePublisher struct {
	mqConn    *RMQConn // 连接
	queueName string   // 生成的队列名称
}

// NewSimplePublisher
// conn：rabbit mq 连接
// queueName:不能为空
// durable：持久化
// autoDelete：自动删除
func NewSimplePublisher(conn *RMQConn, queueName string, durable, autoDelete bool) (*SimplePublisher, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if queueName == "" {
		return nil, QueueNameIsEmpty
	}
	r := &SimplePublisher{
		mqConn:    conn,
		queueName: queueName,
	}
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 1、申请队列,如果队列不存在则创建,存在则跳过
	_, err = channel.QueueDeclare(
		r.queueName,
		durable,    // 是否持久化
		autoDelete, // autoDelete
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	// 2、如果是自动删除队列

	return r, nil
}

// Publish
// message：消息内容
// expirationSecond：过期时间，秒；0 表示永不过期
func (r *SimplePublisher) Publish(message []byte, expirationSecond uint64) (err error) {
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
		"",          // exchange 交换机 simple 模式下默认为空，虽然为空，但其实也是在用的 rabbitmq 当中的 default 交换机运行
		r.queueName, // routing key 在 simple 模式下，将路由 Key 设置为队列的名称
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

type SimpleConsumer struct {
	*baseConsumer
}

// NewSimpleConsumer 创建简单模式下的实例，只需要 queueName 这个参数，其中 exchange 是默认的，key 则不需要。
// conn：rabbit mq 连接
// queueName：队列名，必填参数
// consumer：为空时，自动生成
// durable：是否需要持久化
// autoDelete：是否需要自动删除
func NewSimpleConsumer(conn *RMQConn, queueName, consumer string, durable, autoDelete bool) (IConsumer, error) {
	if conn == nil {
		return nil, ConnIsNil
	}
	if queueName == "" {
		return nil, QueueNameIsEmpty
	}
	r := &SimpleConsumer{}
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

	// 1、申请队列,如果队列不存在则创建,存在则跳过
	q, err := channel.QueueDeclare(
		queueName,  // 队列名
		durable,    // 是否持久化
		autoDelete, // autoDelete
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	r.queueName = q.Name
	return r, nil
}
