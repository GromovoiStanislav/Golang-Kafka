package consumer

import (
	"log"
	"fmt"
	"os"

	"github.com/IBM/sarama"

)

// KafkaConsumer hold sarama consumer
type KafkaConsumer struct {
	Consumer sarama.Consumer
}

// Consume function to consume message from apache kafka
func (c *KafkaConsumer) Consume(topics []string, signals chan os.Signal) {
	chanMessage := make(chan *sarama.ConsumerMessage, 256)

	for _, topic := range topics {
		partitionList, err := c.Consumer.Partitions(topic)
		if err != nil {
			log.Printf("Unable to get partition got error %v\n", err)
			continue
		}
		for _, partition := range partitionList {
			go consumeMessage(c.Consumer, topic, partition, chanMessage)
		}
	}
	// Get topic name.
	topicName := os.Getenv("KAFKA_TOPIC")
	log.Printf("Kafka is consuming [topic: %s]....\n", topicName)

	ConsumerLoop:
	for {
		select {
		case msg := <-chanMessage:
			fmt.Printf("New Message from kafka [topic: %s], message: %v\n", topicName, string(msg.Value))
		case sig := <-signals:
			if sig == os.Interrupt {
				break ConsumerLoop
			}
		}
	}
}

func consumeMessage(consumer sarama.Consumer, topic string, partition int32, c chan *sarama.ConsumerMessage) {
	msg, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Unable to consume partition %v got error %v\n", partition, err)
		return
	}

	defer func() {
		if err := msg.Close(); err != nil {
			log.Printf("Unable to close partition %v: %v\n", partition, err)
		}
	}()

	for {
		msg := <-msg.Messages()
		c <- msg
	}

}