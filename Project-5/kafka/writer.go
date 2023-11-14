package kafka

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Writer struct {
	Writer *kafkago.Writer
}

func NewKafkaWriter() *Writer {
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
	
	writer := &kafkago.Writer{
	
		Addr: kafkago.TCP(KAFKA_HOSTNAME),
		Topic:   "topic-B",
		Transport: &kafkago.Transport{
			TLS: &tls.Config{},
			SASL: mechanism,
		  },
	}
	
	return &Writer{
		Writer: writer,
	}
}

func (k *Writer) WriteMessages(ctx context.Context, messages chan kafkago.Message, messageCommitChan chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			err := k.Writer.WriteMessages(ctx, kafkago.Message{
				Value: m.Value,
			})
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
			case messageCommitChan <- m:
			}
		}
	}
}