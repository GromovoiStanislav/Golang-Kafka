package sarama

import (
	"fmt"
	"crypto/tls"

	"github.com/IBM/sarama"
)

func (s *SaramaLocal) PostMessageInTopic(message string) {
	producer, err := s.newProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}
	defer producer.Close()

	msg := prepareMessage(s.Topic, message)

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		fmt.Printf("%s error occured.", err.Error())
	} else {
		fmt.Printf("Message was saved to partion: %d. Message offset is: %d.\n", partition, offset)
	}
}

func (s *SaramaLocal) newProducer() (sarama.SyncProducer, error) {
	
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = s.Username
	config.Net.SASL.Password = s.Password
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig

	producer, err := sarama.NewSyncProducer(s.Brokers, config)

	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}