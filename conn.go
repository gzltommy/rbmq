package rbmq

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type RMQConn struct {
	conn  atomic.Value // 连接(*amqp.Connection)
	mqURL string       // 连接信息(amqp://账号:密码@主机:端口号/虚拟主机)
}

// NewRMQConn 创建一个 RMQConn 实例
/*
	UserName    = "guest"
	Password    = "guest"
	Host        = "127.0.0.1"
	Port        = "5672"
	VirtualHost = "/"
 mqUrl := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", UserName, Password, Host, Port, VirtualHost)
*/
func NewRMQConn(mqUrl string) (*RMQConn, error) {
	conn, err := amqp.Dial(mqUrl) // 创建 rabbitmq 连接
	if err != nil {
		return nil, err
	}
	mqConn := &RMQConn{
		mqURL: mqUrl,
	}
	mqConn.conn.Store(conn)

	// 开启自动重连
	mqConn.keepAlive()
	return mqConn, nil
}

func (r *RMQConn) GetConn() *amqp.Connection {
	return r.conn.Load().(*amqp.Connection)
}

// keepAlive amqp 断开自动重连
func (r *RMQConn) keepAlive() {
	go func() {
		defer func() {
			if pErr := recover(); pErr != nil {
				buf := debug.Stack()
				os.Stderr.Write(buf)
				log.Println(pErr)
				log.Println(string(buf))
			}
		}()
	Loop:
		err := <-r.GetConn().NotifyClose(make(chan *amqp.Error))
		if err != nil {
			// 异常关闭，重连
			log.Printf("keepAlive: %v \n", err)
			log.Printf("keepAlive: 网络断开，开始自动重连。。。\n")
			for {
				newCon, err := amqp.Dial(r.mqURL)
				if err == nil {
					r.conn.Store(newCon)
					log.Printf("keepAlive: 重连成功！\n")
					goto Loop
				}
				log.Printf("keepAlive：%v \n", err)
				log.Printf("keepAlive：重连失败，1s 后重试！\n")
				time.Sleep(time.Second)
			}
		} else {
			// 正常关闭不重连
			log.Println("keepAlive: rabbitmq connection closing")
		}
	}()
}

// Close 关闭连接
func (r *RMQConn) Close() {
	r.GetConn().Close()
	log.Println("conn is closed!!!")
}

// GetReadyCount 统计正在队列中准备且还未消费的数据
func (r *RMQConn) GetReadyCount(queueName string) (int, error) {
	channel, err := r.GetConn().Channel()
	if err != nil {
		return 0, err
	}
	defer channel.Close()
	queue, err := channel.QueueInspect(queueName)
	if err != nil {
		return 0, err
	}
	return queue.Messages, nil
}

// GetConsumeCount 获取到队列中正在消费的数据，这里指的是正在有多少数据被消费
func (r *RMQConn) GetConsumeCount(queueName string) (int, error) {
	channel, err := r.GetConn().Channel()
	if err != nil {
		return 0, err
	}
	defer channel.Close()

	queue, err := channel.QueueInspect(queueName)
	if err != nil {
		return 0, err
	}
	return queue.Consumers, nil
}

// ClearQueue 清理队列
func (r *RMQConn) ClearQueue(queueName string) error {
	channel, err := r.GetConn().Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	_, err = channel.QueuePurge(queueName, false)
	if err != nil {
		return err
	}
	return nil
}

// DeleteQueue 删除一个 queue 队列
func (r *RMQConn) DeleteQueue(queueName string) error {
	channel, err := r.GetConn().Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	_, err = channel.QueueDelete(
		queueName, // name
		false,     // IfUnused
		false,     // ifEmpty
		true,      // noWait
	)
	if err != nil {
		return err
	}
	return nil
}
