package kafka

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/joho/godotenv"
	//"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Reader struct {
	Reader *kafkago.Reader
}

func NewKafkaReader() *Reader {
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

	dialer := &kafkago.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}


	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{KAFKA_HOSTNAME},
		Topic:   "topic-A",
		GroupID: "group",
		Dialer:  dialer,
	})

	return &Reader{
		Reader: reader,
	}
}

func (k *Reader) FetchMessage(ctx context.Context, messages chan<- kafkago.Message) error {
	for {
		message, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages <- message:
			log.Printf("message fetched and sent to a channel: %v \n", string(message.Value))
		}
	}
}

func (k *Reader) CommitMessages(ctx context.Context, messageCommitChan <-chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
		case msg := <-messageCommitChan:
			err := k.Reader.CommitMessages(ctx, msg)
			if err != nil {
				//return errors.Wrap(err, "Reader.CommitMessages")
			}
			log.Printf("committed an msg: %v \n", string(msg.Value))
		}
	}
}