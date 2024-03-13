package services

import (
	"log"
)

type EventHandler interface {
	Handle(topic string, eventBytes []byte)
}

type readEventHandler struct {
}

func NewReadEventHandler() EventHandler {
	return readEventHandler{}
}

func (obj readEventHandler) Handle(topic string, eventBytes []byte) {
	switch topic {
	
	case "test-topic":
		log.Printf("[%v] %#v", topic, string(eventBytes))
	

	default:
		log.Println("no event handler")
	}
}