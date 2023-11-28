package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)


func newKafkaWriter(topic string) *kafka.Writer {
	// Загрузите переменные среды из файла .env
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	mechanism, err := scram.Mechanism(scram.SHA256, KAFKA_USERNAME, KAFKA_PASSWORD)
	if err != nil {
		log.Fatalln(err)
	}

	return&kafka.Writer{
		Addr: kafka.TCP(KAFKA_HOSTNAME),
		Topic: topic,
		Transport: &kafka.Transport{
			TLS:  &tls.Config{},
			SASL: mechanism,
		},
		Balancer: &kafka.LeastBytes{},
	}
}


func main() {
	writer := newKafkaWriter("my-topic")
	defer writer.Close()

	fmt.Println("start producing ... !!")

	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(uuid.New())),
		}

		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}
		
		time.Sleep(1 * time.Second)
	}
}
