package queue

import (
	"log"
	"testing"
	"time"
)

func BenchmarkQueue(b *testing.B) {
	q, err := New("myqueque")
	if err != nil {
		log.Fatalln(err.Error())
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bytes := []byte(time.Now().String())

		if err := q.Push(bytes); err != nil {
			log.Fatalln(err.Error())
		}
	}
	b.StopTimer()
}
