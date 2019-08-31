package queue

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	queueProto "github.com/xumc/mini-queue/proto"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type Server interface {
	Start() error
	NewQueue(queueName string) (*Queue, error)
}

type server struct {
	host string
	port int32
	data string
	queues map[string]*Queue
	queuesMutex sync.RWMutex
}

func NewServer(host string, port int32, data string) (Server, error) {
	err := os.MkdirAll(data, 777)
	if err != nil {
		return nil, err
	}
	return &server{
		host:        host,
		port:        port,
		data:        data,
		queues:      make(map[string]*Queue),
		queuesMutex: sync.RWMutex{},
	}, nil
}

func (s *server) NewQueue(queueName string) (*Queue, error) {
	file, err := os.Create(filepath.Join(s.data, queueName))
	if err != nil {
		return nil, err
	}

	q := &Queue{
		file:    file,
		rwMutex: sync.RWMutex{},
	}

	s.queuesMutex.Lock()
	s.queues[queueName] = q
	s.queuesMutex.Unlock()

	return q, nil
}

func (s *server) Start() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			os.Exit(1)
		}

		go s.handleConnection(conn)
	}
}

func (s *server) handleConnection(conn net.Conn) {
	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		if err := recover(); err != nil {
			log.Println("connection error recovered", err)
			cancel()
			conn.Close()
		}
	}()

	defer conn.Close()

	connChan := make(chan []byte)
	go func(innerCtx context.Context, innerConn net.Conn, innerConnChan chan []byte) {
		for {
			select {
			case <- innerCtx.Done():
				return
			case writeData := <-innerConnChan:
				_, err := innerConn.Write(writeData)
				if err != nil {
					log.Fatalln("server write data error :", err)
				}
			}


		}
	}(ctx, conn, connChan)

	for {
		select {
		case <- ctx.Done():
			return
		default:
			reqBytes, err := readOneMsg(conn)
			if err != nil {
				log.Fatalln(err)
			}
			s.handleRequest(ctx, connChan, reqBytes)
		}

	}
}

func (s *server) handleRequest(_ context.Context, connChan chan []byte, reqBytes []byte) {
	pb := &queueProto.Request{}
	if err := proto.Unmarshal(reqBytes, pb); err != nil {
		log.Fatalln(err.Error())
	}

	queue, ok := s.queues[pb.Queue]
	if !ok {
		newQueue, err := s.NewQueue(pb.Queue)
		if err != nil {
			log.Fatalln(err)
		}

		queue = newQueue
	}

	switch pb.Op {
	case queueProto.Op_PUSH:
		err := queue.Push(pb.Data)
		if err != nil {
			log.Fatalln(err)
		}

		// push ack
		data, err := rawDataToResponseData(pb.Id, pb.Queue, queueProto.Op_PUSH_ACK, []byte{})
		if err != nil {
			log.Fatalln(err)
		}
		connChan <- data

		log.Debug(pb.Id, "server send ack")

	case queueProto.Op_POP:
		go func() {
			consumer := queue.NewConsumer()
			for {
				rawData, err := consumer.Pop()
				if err != nil {
					log.Fatalln(err)
				}

				data, err := rawDataToResponseData(pb.Id, pb.Queue, queueProto.Op_POP_DATA, rawData)
				if err != nil {
					log.Fatalln(err)
				}

				connChan <- data
			}
		}()
	}
}

func rawDataToResponseData(id int64, queueName string, op queueProto.Op, data []byte) ([]byte, error) {
	pb := &queueProto.Response{
		Id:   id,
		Queue: queueName,
		Op:   op,
		Data: data,
	}

	pbBytes, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}

	size := int64(len(pbBytes) + ElementMetadataSize)

	respBytes := append(int64ToBytes(size), pbBytes...)
	return respBytes, nil
}
