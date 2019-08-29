package queue

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	queueProto "github.com/xumc/mini-queue/queue/proto"
	"log"
	"net"
	"os"
	"sync"
)

type Server interface {
	Start() error
	NewQueue(queueName string) (*Queue, error)
}

type server struct {
	host string
	port int32

	queuesMutex sync.RWMutex

}

func NewServer(host string, port int32) Server {
	return &server{
		host: host,
		port: port,
		queuesMutex: sync.RWMutex{},
	}
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
		go s.handleRequest(conn)
	}
}

func (s *server) NewQueue(queueName string) (*Queue, error) {
	file, err := os.Create(queueName)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		file:    file,
		rwMutex: sync.RWMutex{},
	}

	s.queuesMutex.Lock()
	queues[queueName] = q
	s.queuesMutex.Unlock()

	return q, nil
}

type Request struct {
	size int64
	pb []byte
}

func (s *server) handleRequest(conn net.Conn) {
	defer conn.Close()

	for {
		reqBytes, err := readOneMsg(conn)
		if err != nil {
			if err == ReaderEOFErr {
				return
			}

			log.Fatalln(err)
		}

		s.doHandleRequest(conn, reqBytes)
	}
}

func (s *server) doHandleRequest(conn net.Conn, reqBytes []byte) error {
	pb := &queueProto.Request{}
	if err := proto.Unmarshal(reqBytes[elementMetadataSize:], pb); err != nil {
		log.Fatalln(err.Error())
	}

	log.Println("server recv: ", pb.Queue, pb.Op, pb.Data)

	queue := queues[pb.Queue]

	switch pb.Op {
	case queueProto.Op_PUSH:
		return queue.Push(pb.Data)
	case queueProto.Op_POP:
		go func() {
			consumer := queue.NewConsumer()
			for {
				rawData, err := consumer.Pop()
				if err != nil {
					log.Fatalln(err)
				}

				data, err := rawDataToResponseData(pb.Id, queueProto.Op_POP_DATA, rawData)
				if err != nil {
					log.Fatalln(err)
				}

				log.Println("server send : ", data)

				_, err = conn.Write(data)
				if err != nil {
					log.Fatalln(err)
				}
			}
		}()
	}

	return nil
}

func rawDataToResponseData(id int64, op queueProto.Op, data []byte) ([]byte, error) {
	pb := &queueProto.Response{
		Id: id,
		Op: op,
		Data: data,
	}

	pbBytes, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}

	size := int64(len(pbBytes) + elementMetadataSize)

	respBytes := append(int64ToBytes(size), pbBytes...)
	return respBytes, nil
}
