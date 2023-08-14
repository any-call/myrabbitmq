package myrabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"
	"time"
)

func Test_simpler(t *testing.T) {
	var addr string = "amqp://19EQqn3TqYdXaudMFGsdcUvUJBsk:dJV4QngHGJRDzy7VqzfUbsSBENrLFrSzLoYTmvne56E@117.41.25.5:5672/"
	var simpleQueue string = "simple"
	s, err := NewSimple(addr)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	r, err := NewSimple(addr)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	var fn ReceivedMsg = func(d []byte) {
		fmt.Println("fn received: ", string(d))
	}

	if err := r.Receive(simpleQueue, fn); err != nil {
		t.Error("start received fn2 :", err)
	}

	go func() {
		for {
			tmpstr := fmt.Sprintf("fn1 %v", time.Now().Second())
			if err := s.Send(simpleQueue, time.Second*5, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(tmpstr),
			}); err != nil {
				t.Error("fn1 send err:", err)
				break
			} else {
				//t.Logf("fn1 send %s ok \n", tmpstr)
			}

			time.Sleep(time.Second)
		}
	}()

	var forever chan struct{}
	<-forever
}

func Test_worker(t *testing.T) {
	var addr string = "amqp://9EQqn3TqYdXaudMFGsdcUvUJBsk:dJV4QngHGJRDzy7VqzfUbsSBENrLFrSzLoYTmvne56E@117.41.25.5:5672/"
	var simpleQueue string = "worker"
	s, err := NewWorker(addr)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	customer1, err := NewWorker(addr)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	customer2, err := NewWorker(addr)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	if err := customer1.Consumer(simpleQueue, func(d []byte) {
		fmt.Println("fn1 received: ", string(d))
		//time.Sleep(time.Second)
	}); err != nil {
		t.Error("start received fn1 :", err)
	}

	if err := customer2.Consumer(simpleQueue, func(d []byte) {
		fmt.Println("fn2 received: ", string(d))
		//time.Sleep(time.Second * 2)
	}); err != nil {
		t.Error("start received fn1 :", err)
	}

	go func() {
		for {
			tmpstr := fmt.Sprintf("fn1 %v", time.Now().Second())
			if err := s.Create(simpleQueue, time.Second*5, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(tmpstr),
			}); err != nil {
				t.Error("create err:", err)
				break
			} else {
				//t.Logf("create %s ok \n", tmpstr)
			}

			time.Sleep(time.Millisecond * 400)
		}
	}()

	var forever chan struct{}
	<-forever
}

func Test_routing(t *testing.T) {
	var addr string = "amqp://9EQqn3TqYdXaudMFGsdcUvUJBsk:dJV4QngHGJRDzy7VqzfUbsSBENrLFrSzLoYTmvne56E@117.41.25.5:5672/"
	swhName := "myswitch002"
	s, err := NewRouting(addr, swhName)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	r1, err := NewRouting(addr, swhName)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	r2, err := NewRouting(addr, swhName)
	if err != nil {
		t.Error("init rabbitmq err: ", err)
		return
	}

	go func() {
		if err := r1.Subscribe("key1", func(d []byte) {
			fmt.Println("fn1 received: ", string(d))
		}); err != nil {
			t.Error("start received fn1 :", err)
		}
	}()

	go func() {
		if err := r2.Subscribe("key2", func(d []byte) {
			fmt.Println("fn2 received: ", string(d))
			//time.Sleep(time.Second * 2)
		}); err != nil {
			t.Error("start received fn1 :", err)
		}
	}()

	go func() {
		var count int = 0
		for {
			if (count & 2) == 0 {
				tmpstr := fmt.Sprintf("key1 %d %v", count, time.Now().Second())
				if err := s.Publish("key1", time.Second*5,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(tmpstr),
					}); err != nil {
					t.Error("create err:", err)
					break
				}
			} else {
				tmpstr := fmt.Sprintf("key2%d %v", count, time.Now().Second())
				if err := s.Publish("key2", time.Second*5, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(tmpstr),
				}); err != nil {
					t.Error("create err:", err)
					break
				}
			}
			count++

			time.Sleep(time.Millisecond * 400)
		}
	}()

	var forever chan struct{}
	<-forever
}
