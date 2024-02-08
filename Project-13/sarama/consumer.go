package sarama

import (
	"fmt"
	"crypto/tls"

	"github.com/IBM/sarama"
)

func (s *SaramaLocal) GetMessagesFromTopic() {

	config := sarama.NewConfig()

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = s.Username
	config.Net.SASL.Password = s.Password
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig


	consumer, err := sarama.NewConsumer(s.Brokers, config)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}
	defer consumer.Close()

	subscribe(s.Topic, consumer)

	select {}
}

func subscribe(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Erro ao listar as particoes ", err)
	}
	
	initialOffset := sarama.OffsetOldest

	for _, partition := range partitionList {
		
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)
			}
		}(pc)
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	fmt.Println(string(message.Value))
}