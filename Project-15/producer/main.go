package main

import (
	"log"
	"os"
	"crypto/tls"

	"github.com/gofiber/fiber/v2"
	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"

	"kafka-exapmple/producer/controllers"
	"kafka-exapmple/producer/services"
)

func main() {
	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	brokers := []string{KAFKA_HOSTNAME}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = KAFKA_USERNAME
	config.Net.SASL.Password = KAFKA_PASSWORD
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig

	
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Panic(err)
	}
	defer producer.Close()

	eventProducer := services.NewEventProducer(producer)
	accountService := services.NewAccountService(eventProducer)
	accountController := controllers.NewAccountController(accountService)

	app := fiber.New()

	app.Post("/openAccount", accountController.OpenAccount)
	app.Post("/depositFund", accountController.DepositFund)
	app.Post("/withdrawFund", accountController.WithdrawFund)
	app.Post("/closeAccount", accountController.CloseAccount)

	app.Listen(":3000")

}
