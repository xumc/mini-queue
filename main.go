package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"github.com/xumc/mini-queue/queue"
	"os"
)

var h = flag.String("host", "localhost", "host or IP")
var p = flag.Int("port", 8899, "port")
var d = flag.String("data", ".", "data path")

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

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

