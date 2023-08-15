package myrabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type (
	ReceivedMsg func(d []byte)

	Simple interface {
		Send(qName string, timeout time.Duration, pubInfo amqp.Publishing) error
		Receive(qName string, f ReceivedMsg) error
	}

	Worker interface {
		Create(qName string, timeout time.Duration, pubInfo amqp.Publishing) error
		Consumer(qName string, f ReceivedMsg) error
	}

	PSubscribe interface {
		Publish(timeout time.Duration, pubInfo amqp.Publishing) error
		Subscribe(f ReceivedMsg) error
	}

	Routing interface {
		Publish(rKey string, timeout time.Duration, pubInfo amqp.Publishing) error
		Subscribe(rKey string, f ReceivedMsg) error
	}
)

type base struct {
	sync.Mutex
	conn          *amqp.Connection
	ch            *amqp.Channel
	connectionStr string
	onclose       func()
}

func newBase(connectionStr string) (*base, error) {
	b := &base{connectionStr: connectionStr}
	if err := b.initChannel(); err != nil {
		return nil, err
	}

	return b, nil
}

// InitChannel 初始化一个通道,当前程序的所有操作都在这个通道中进行
func (b *base) initChannel() error {
	var err error
	if err = b.EnsureConnect(); err != nil {
		return err
	}

	if b.ch, err = b.conn.Channel(); err != nil {
		return err
	}

	chCloseErr := make(chan *amqp.Error, 0)
	b.ch.NotifyClose(chCloseErr)
	go func(ch chan *amqp.Error) {
		select {
		case <-ch:
			{
				if b.onclose != nil {
					for b.ch == nil || b.ch.IsClosed() {
						b.onclose()
						fmt.Println("conn is close ,wait 1 sec will re-conn")
						time.Sleep(time.Second)
					}
				}
			}

		}
	}(chCloseErr)

	return nil
}

// EnsureConnect 确保连接
func (b *base) EnsureConnect() error {
	b.Lock()
	if b.conn != nil && !b.conn.IsClosed() {
		b.Unlock()
		return nil
	}
	b.Unlock()
	return b.connect()
}

// connect 连接amqp服务器
func (b *base) connect() error {
	b.Lock()
	defer b.Unlock()
	var err error
	b.conn, err = amqp.Dial(b.connectionStr)
	if err != nil {
		fmt.Printf("connect failed,err is %v ", err)
		b.conn = nil
		return err
	}
	return nil
}

// 声明一个队列.
// 注: 若不事先声明队列,便向这个队列中发送或者订阅,会由报错
func (b *base) setQueue(qName string, durable, autoDelete, exclusive, nowait bool) (ret string, err error) {
	if q, err := b.ch.QueueDeclare(qName, durable, autoDelete, exclusive, nowait, nil); err != nil {
		return "", err
	} else {
		return q.Name, err
	}

}

// 将指定队列和指定路由绑定到默认交换机中
func (b *base) setBind(sName, qName string, rKey string) error {
	return b.ch.QueueBind(qName, rKey, sName, false, nil)
}

// 队列消失调度 Qos
func (b *base) setQos(prefetchCount, prefetchSize int, global bool) error {
	return b.ch.Qos(prefetchCount, prefetchSize, global)
}
