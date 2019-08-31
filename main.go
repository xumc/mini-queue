package main

import (
	"flag"
	"github.com/xumc/mini-queue/queue"
	"log"
)

var h = flag.String("host", "localhost", "host or IP")
var p = flag.Int("port", 8899, "port")
var d = flag.String("data", ".", "data path")

func main() {
	flag.Parse()
	var host = *h
	var port = *p
	var data = *d

	log.Println("host: ", host)
	log.Println("port: ", port)
	log.Println("data: ", data)

	server, err := queue.NewServer(host, int32(port), data)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println(server.Start())
}

