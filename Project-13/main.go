package main

import (
	"os"

	_ "github.com/joho/godotenv/autoload"
	samaraLocal "kafka-example/sarama"
)

var (
	SaramaLocal = samaraLocal.NewSaramaLocal(
		[]string{os.Getenv("KAFKA_HOSTNAME")},
		os.Getenv("KAFKA_USERNAME"),
		os.Getenv("KAFKA_PASSWORD"),
		"my-topic")
)

func main() {
	SaramaLocal.GetMessagesFromTopic()

	//SaramaLocal.PostMessageInTopic("New message 5")
}