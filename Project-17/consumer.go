package main

import (
	"log"
	"fmt"
	"os"
	"os/signal"
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
	config.Consumer.Return.Errors = true

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = KAFKA_USERNAME
	config.Net.SASL.Password = KAFKA_PASSWORD
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig


	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalln("Failed to start consumer:", err)
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic
	partitionConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalln("Failed to start partition consumer:", err)
	}
	defer partitionConsumer.Close()

	// Handle messages
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message: %s\n", msg.Value)
		case err := <-partitionConsumer.Errors():
			log.Println("Error:", err)
		case <-signals:
			return
		}
	}
}
