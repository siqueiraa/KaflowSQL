package state

import "time"

type Record struct {
	Timestamp time.Time
	Payload   []byte
}

type Manager interface {
	Put(topic, key string, record Record) error
	Get(topic, key string) ([]Record, error)
	Cleanup() error
}
