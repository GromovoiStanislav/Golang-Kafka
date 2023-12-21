package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/linkedin/goavro/v2"
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

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{KAFKA_HOSTNAME},
		Topic:   "my-topic",
		Dialer:  dialer,
	})


	// Создание и инициализация объекта схемы avro
	schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatalf("Error creating Avro codec: %v", err)
	}

	// Создание сообщения avro
	message := map[string]interface{}{
		"name": "John Doe",
		"age": 30,
	}

	// Сериализация сообщения в бинарный формат avro
	avroBytes, err := codec.BinaryFromNative(nil, message)
		if err != nil {
			log.Fatalf("Error encoding Avro data: %v", err)
	}


	// Отправка сообщения в Kafka
	err = w.WriteMessages(context.Background(),kafka.Message{Value: avroBytes})
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
    	log.Fatal("failed to close writer:", err)
	}
}