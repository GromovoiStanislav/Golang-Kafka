package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

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
	
	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{KAFKA_HOSTNAME},
		GroupID: "consumer-group-id",
		Topic:   "my-topic",
		Dialer:  dialer,
	})
	// r.SetOffset(42)

	ctx := context.Background()

	//Reading all messages within a time range
	startTime := time.Now().Add(-time.Hour)
	endTime := time.Now()
	r.SetOffsetAt(ctx, startTime)
	
	
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		if m.Time.After(endTime) {
			break
		}

		log.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset,  string(m.Value))
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}
	
	if err := r.Close(); err != nil {
    	log.Fatal("failed to close reader:", err)
	}
}