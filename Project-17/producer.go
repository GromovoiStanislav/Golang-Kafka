package main

import (
	"log"
	"fmt"
	"os"
	"crypto/tls"

	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"

	. "kafka-example/common"
)

var (
	topic = "my-topic"
)

func main() {
	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	brokers := []string{KAFKA_HOSTNAME}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = KAFKA_USERNAME
	config.Net.SASL.Password = KAFKA_PASSWORD
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig


	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalln("Failed to start producer:", err)
	}
	defer producer.Close()

	// Publish a message to the Kafka topic
    msg := &sarama.ProducerMessage{
        Topic: "test-topic",
        Value: sarama.StringEncoder("Hello, Kafka!"),
    }

    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        log.Println("Failed to send message:", err)
    } else {
        fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
    }
}
