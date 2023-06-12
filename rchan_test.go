package rchan

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestXxx(t *testing.T) {
	rc := New(
		WithMaxLen(2),
		WithRedisConfig("192.168.1.222:6379", "", 0),
	)

	go func() {
		for {
			l, _ := rc.Len()
			log.Println("rc.length:", l)
			time.Sleep(time.Second * 1)
		}
	}()

	go func() {
		for {
			_, err := rc.Push(time.Now().Unix())
			if err == nil {
				fmt.Println("push success")
			}
		}
	}()

	go func() {
		for {
			v, _ := rc.Pop()
			if v != "" {
				fmt.Println(v)
			}
			time.Sleep(time.Millisecond * 500)
		}
	}()
	<-make(chan struct{})
}
