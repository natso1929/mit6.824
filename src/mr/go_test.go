package mr

import (
	"fmt"
	"testing"
	"time"
)

func TestClosure(t *testing.T) {
	message := "Hello"

	go func() {
		fmt.Println(message) // 访问外部变量 message
	}()

	time.Sleep(time.Second) // 等待 goroutine 执行完毕
}

func TestTime(t *testing.T) {
	t1 := time.Now()            //获取本地现在时间
	time.Sleep(time.Second * 2) //延时2秒
	t2 := time.Now()
	d := t2.Sub(t1) //两个时间相减
	fmt.Println(d)

}

func TestDuration(t *testing.T) {

	duration := time.Second * 5 // 5秒后执行任务

	<-time.After(duration)

	// 执行任务的代码
	fmt.Println("任务执行")

}
