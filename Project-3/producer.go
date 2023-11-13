package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"


	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func main() {
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
	

	w := &kafka.Writer{
		Addr: kafka.TCP(KAFKA_HOSTNAME),
		Topic:   "my-topic",
		Transport: &kafka.Transport{
			TLS: &tls.Config{},
			SASL: mechanism,
		  },
		Balancer: &kafka.LeastBytes{},
	}
	
	err = w.WriteMessages(context.Background(),
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
		)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
    	log.Fatal("failed to close writer:", err)
	}
}