package main

import (
	"fmt"
	"os"
	"context"
	"crypto/tls"

	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"

	"kafka-example/consumer-group/services"
	. "kafka-example/common"
)


func main() {
	topic := "test-topic"
	group := "test-group"

	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME") 
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME") 
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD") 

	brokers := []string{KAFKA_HOSTNAME}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = KAFKA_USERNAME
	config.Net.SASL.Password = KAFKA_PASSWORD
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig

	consumer, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()


	readEventHandler := services.NewReadEventHandler()
	readConsumerHandler := services.NewConsumerHandler(readEventHandler)

	var topics = []string{
		topic,
	}


	fmt.Println("Read consumer started...")
	for {
		consumer.Consume(context.Background(), topics, readConsumerHandler)
	}
}
