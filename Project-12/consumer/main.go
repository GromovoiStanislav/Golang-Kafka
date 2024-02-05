package main

import (
	"fmt"
	"log"
	"os"
	"crypto/tls"
	"os/signal"

	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"
)


var (
	messageCount = 0
)

func main() {

	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	topic := "my-topic"

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

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}()


	// fmt.Println(config.Consumer.Offsets.AutoCommit.Enable)

	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Erro ao listar as particoes ", err)
	}
	
	doneCh := make(chan struct{})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)


	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)

		go func(pc sarama.PartitionConsumer) {
		
			for {
				select {
				case err := <-pc.Errors():
					log.Println(err)
				case msg := <-pc.Messages():
					messageCount++
					fmt.Printf("Received message: %s %d %s #%d\n",msg.Key,msg.Partition, msg.Value, messageCount)
				
		
				case <-signals:
					log.Println("Interrupt is detected")
					doneCh <- struct{}{}
				}
			}

		}(pc)
	}








	<-doneCh
	log.Println("Processed", messageCount, "messages")
}
