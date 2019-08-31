package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/xumc/mini-queue/queue"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

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

	for i := 0; i < 2; i++ {
		// pop
		go func(innerCtx context.Context, gid int) {
			dataChan, err := client.Pop(fmt.Sprintf("myqueue-%d", gid))

			for {
				select {
				case <- innerCtx.Done():
					return
				case data := <-dataChan:
					log.Println("client recv: ", string(data))
				}
			}

			if err != nil {
				log.Debug(err.Error())
			}

		}(ctx, i)
	}

	//push
	for i := 0; i < 2; i++ {
		go func(innerCtx context.Context, gid int) {
			for {
				select {
				case <- innerCtx.Done():
					return
				case <- time.Tick(time.Second):
					data := []byte(strconv.Itoa(gid) + " => " + time.Now().String() + "\n")
					log.Debug("client send: ", string(data))
					err = client.Push(fmt.Sprintf("myqueue"), data)
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
