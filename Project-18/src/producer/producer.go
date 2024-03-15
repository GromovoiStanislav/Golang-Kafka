package producer

import (
	"log"
	"fmt"

	"github.com/IBM/sarama"
)

// KafkaProducer hold kafka producer session
type KafkaProducer struct {
	Producer sarama.SyncProducer
}

// SendMessage function to send message into kafka
func (p *KafkaProducer) SendMessage(topic, msg string) error {

	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}

	partition, offset, err := p.Producer.SendMessage(kafkaMsg)
	if err != nil {
		log.Printf("Send message error: %v\n", err)
		return err
	}

	fmt.Printf("Send message success, Topic %v, Partition %v, Offset %d\n", topic, partition, offset)
	return nil
}