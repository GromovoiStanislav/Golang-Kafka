package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func producerHandler(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}

		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}

		err = kafkaWriter.WriteMessages(req.Context(), msg)
		if err != nil {
			wrt.Write([]byte(err.Error()))
			log.Fatalln(err)
		}
	})
}

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
	kafkaWriter := newKafkaWriter("my-topic")
	defer kafkaWriter.Close()

	// Add handle func for producer.
	http.HandleFunc("/", producerHandler(kafkaWriter))

	// Run the web server.
	fmt.Println("start producer-api ... !!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
