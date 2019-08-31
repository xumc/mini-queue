package main

import (
	"context"
	"fmt"
	"github.com/xumc/mini-queue/queue"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	run()
}

func run() {
	ctx, cancel := context.WithCancel(context.Background())

	client, err := queue.NewClient("localhost", 8899)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer client.Close()

	for i := 0; i < 10; i++ {
		// pop
		go func(innerCtx context.Context, gid int) {
			dataChan, err := client.Pop(fmt.Sprintf("myqueue-%d", gid))

			for {
				select {
				case <- innerCtx.Done():
					return
				case data := <-dataChan:
					fmt.Println("client recv: ", string(data))
				}
			}

			if err != nil {
				log.Fatalln(err.Error())
			}

		}(ctx, i)
	}

	//push
	for i := 0; i < 10; i++ {
		go func(innerCtx context.Context, gid int) {
			for {
				select {
				case <- innerCtx.Done():
					return
				case <- time.Tick(time.Second):
					data := []byte(time.Now().String() + "\n")
					fmt.Println("client send: ", string(data))
					err = client.Push(fmt.Sprintf("myqueue-%d", gid), data)
					if err != nil {
						log.Fatalln(err.Error())
					}
				}

			}
		}(ctx, i)
	}

	exitSig := make(chan os.Signal)
	signal.Notify(exitSig, syscall.SIGKILL)
	<-exitSig
	cancel()
}
