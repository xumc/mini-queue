package main

import (
	"github.com/xumc/mini-queue/queue"
	"log"
)

func main() {
	server := queue.NewServer("localhost", 8899)

	_, err := server.NewQueue("myqueue")
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println(server.Start())
}

