package main

import (
	"fmt"
	"log"
	"os"
	"crypto/tls"
	"os/signal"
	"sync"


	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"
)

func main() {

	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")



	// Указываем адрес Kafka брокера
	brokerList := []string{KAFKA_HOSTNAME}

	// Создаем конфигурацию для потребителя
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = KAFKA_USERNAME
	config.Net.SASL.Password = KAFKA_PASSWORD
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	//config.Consumer.Offsets.Initial = sarama.OffsetOldest

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig

	// Создаем нового потребителя
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	log.Println("consumer created")
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Error closing consumer: %v", err)
		}
	}()
	log.Println("commence consuming")

	// Указываем топик, который будем читать
	topic := "my-topic"

	// Создаем партицию и начинаем читать сообщения
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating partition consumer: %v", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalf("Error closing partition consumer: %v", err)
		}
	}()

	// Используем канал для обработки сигнала завершения
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Используем WaitGroup для ожидания завершения
	var wg sync.WaitGroup
	wg.Add(1)

	// Запускаем горутину для чтения сообщений
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				fmt.Printf("Received message: %s\n", msg.Value)
			case err := <-partitionConsumer.Errors():
				log.Printf("Error: %v\n", err)
			case <-signals:
				return
			}
		}
	}()


	// Отправляем сообщение
	{
		syncProducer, err := sarama.NewSyncProducer(brokerList, config)
		if err != nil {
			log.Fatalln("failed to create producer: ", err)
		}
		partition, offset, err := syncProducer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder("test_message"),
		})
		if err != nil {
			log.Fatalln("failed to send message to ", topic, err)
		}
		log.Printf("wrote message at partition: %d, offset: %d", partition, offset)
		_ = syncProducer.Close()
	}


	// Ожидаем завершения работы по сигналу
	wg.Wait()
	fmt.Println("Consumer stopped")
}
