package main

import (
	"log"
	"os"
	"time"
	"crypto/tls"


	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"

	. "kafka-example/common"
	"kafka-example/src/consumer"
)


func main() {
	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	kafkaConfig := getKafkaConfig(KAFKA_USERNAME, KAFKA_PASSWORD)

	// consumers, err := sarama.NewConsumer([]string{"kafka:9092"}, kafkaConfig)
	consumers, err := sarama.NewConsumer([]string{ KAFKA_HOSTNAME }, kafkaConfig)
	if err != nil {
		log.Fatalln("Error create kakfa consumer got error %v", err)
	}
	defer consumers.Close()


	kafkaConsumer := &consumer.KafkaConsumer{
		Consumer: consumers,
	}

	signals := make(chan os.Signal, 1)

	// Get topic name.
	topicName := os.Getenv("KAFKA_TOPIC")
	kafkaConsumer.Consume([]string{topicName}, signals)
}


func getKafkaConfig(username, password string) *sarama.Config {
	
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Net.WriteTimeout = 5 * time.Second

	if username != "" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Handshake = true
		kafkaConfig.Net.SASL.User = username
		kafkaConfig.Net.SASL.Password = password
		kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

		tlsConfig := tls.Config{}
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tlsConfig
	}

	return kafkaConfig
}