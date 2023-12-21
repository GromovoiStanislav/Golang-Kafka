package main

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/linkedin/goavro/v2"
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
	
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		// Декодирование бинарных данных обратно в объект
		native, _, err := codec.NativeFromBinary(m.Value)
		if err != nil {
			log.Println("Error decoding Avro data:", err)
			continue
		}

		decodedData := native.(map[string]interface{})
		log.Printf("Decoded:  %v", decodedData)
	}
	
	if err := r.Close(); err != nil {
    	log.Fatal("failed to close reader:", err)
	}
}