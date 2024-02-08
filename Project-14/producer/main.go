package main

import (
	"log"
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
	publisher, err := getProducer()
	if err != nil {
		log.Panic(err)
	}
	defer publisher.Close()

	msg := getMensage("Hello Kafka")

	partition, offset, _ := publisher.SendMessage(msg)
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}


func getProducer() (sarama.SyncProducer, error) {
	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	brokers := []string{KAFKA_HOSTNAME}


	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
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

	return sarama.NewSyncProducer(brokers, config)
}


func getMensage(msg string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(msg),
	}
}