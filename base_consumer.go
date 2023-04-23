package rbmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"
)

/*
关于 Qos 的 prefetch
(1)实际使用 RabbitMQ 过程中，如果完全不配置 Qos，这样 Rabbit 会尽可能快速地发送队列中的所有消息到 client 端。
让 consumer 在本地缓存所有的 message，从而极有可能导致OOM或者导致服务器内存不足影响其它进程的正常运行。
所以我们需要通过设置 Qos 的 prefetch count 来控制 consumer 的流量。同时设置得当也会提高 consumer 的吞吐量。

(2)prefetch 与消息投递
prefetch 允许为每个 consumer 指定最大的 unacked messages 数目。简单来说就是用来指定一个 consumer 一次可以
从 Rabbit 中获取多少条 message 并缓存在 client 中(RabbitMQ 提供的各种语言的 client library)。一旦缓冲区满了，
Rabbit 将会停止投递新的 message 到该 consumer 中直到它发出 ack。

假设 prefetch 值设为 10，共有两个 consumer。意味着每个 consumer 每次会从 queue 中预抓取 10 条消息到本地缓存着等待消费。
同时该 channel 的 unacked 数变为 20。而 Rabbit 投递的顺序是，先为 consumer1 投递满 10 个 message，再往 consumer2 投递
10 个 message。如果这时有新 message 需要投递，先判断 channel 的 unacked 数是否等于 20，如果是则不会将消息投递到 consumer 中，
message 继续呆在 queue 中。之后其中 consumer 对一条消息进行 ack，unacked 此时等于 19，Rabbit 就判断哪 个consumer 的 unacked 少于 10，
就投递到哪个 consumer 中。

总的来说，consumer 负责不断处理消息，不断 ack，然后只要 unacked 数少于 prefetch * consumer 数目，broker 就不断将消息投递过去。
*/

const (
	DefaultPrefetchCount = 500
)

type ConsumeHandler func(payload []byte) error

type IConsumer interface {
	Consume(ctx context.Context, handler ConsumeHandler) (err error)
}

type baseConsumer struct {
	iC            IConsumer
	mqConn        *RMQConn //连接
	prefetchCount int
	queueName     string // 队列名
	consumer      string // 消费名
}

// Consume  该方法会阻塞调用，一般都用一个独立的 goroutine 调用该方法
func (r *baseConsumer) Consume(ctx context.Context, handler ConsumeHandler) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			fmt.Fprintln(os.Stderr, pErr)
			buf := debug.Stack()
			fmt.Fprintln(os.Stderr, string(buf))
			log.Println(pErr)
			log.Printf("pErr:%v.stack:%s", pErr, string(buf))
			err = fmt.Errorf("%v", pErr)
		}
	}()
Recon:
	var isConnClosed bool
	isConnClosed, err = r.consumeHandle(ctx, handler)
	if err != nil {
		return err
	}
	// 如果是连接被关闭才返回，且是异常断网，则等待重连后继续监听消费
	if isConnClosed && !r.mqConn.IsnNormalClose() {
		for r.mqConn.GetConn().IsClosed() {
			time.Sleep(time.Second)
		}
		goto Recon
	}
	return nil
}

func (r *baseConsumer) consumeHandle(ctx context.Context, handler ConsumeHandler) (bool, error) {
	channel, err := r.mqConn.GetConn().Channel()
	if err != nil {
		return false, err
	}
	defer channel.Close()

	err = channel.Qos(
		r.prefetchCount, // prefetch count
		0,               // prefetch size
		false,           // global
	)
	if err != nil {
		return false, err
	}

	// 消费消息
	deliveryChan, err := channel.Consume(
		r.queueName, // 引用前面的队列名
		r.consumer,  // 消费者名字，不填自动生成一个
		false,       // 自动向队列确认消息已经处理
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return false, err
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("consumeHandle：消费者退出监听！")
			return false, nil
		case d, ok := <-deliveryChan:
			if ok {
				err = handler(d.Body)
				if err != nil {
					if err = d.Nack(false, true); err != nil {
						log.Printf("deliver.Nack: %s\n", err)
					}
				} else {
					if err = d.Ack(false); err != nil {
						log.Printf("deliver.Ack: %s\n", err)
					}
				}
			} else {
				// 通道被关闭，可能是异常断网，也可能是正常关闭网络
				log.Println("consumeHandle：deliveryChan closed！")
				return true, nil
			}
		}
	}
}
