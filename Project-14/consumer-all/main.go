package main

import (
	"fmt"
	"log"
	"os"
	"crypto/tls"


	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"

	. "kafka-example/common"
)


func main() {

	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	topic := "my-topic"

	brokers := []string{KAFKA_HOSTNAME}


	config := sarama.NewConfig()

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
		log.Panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}()


	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	for _, partition := range partitionList {
		
		partitionConsumer, _ := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)

		go func(partitionConsumer sarama.PartitionConsumer) {

			channel := partitionConsumer.Messages()

			for {
				msg := <-channel
				fmt.Println("partition", msg.Partition, string(msg.Value)) 
			}

		}(partitionConsumer)
	}

	select {}

}
