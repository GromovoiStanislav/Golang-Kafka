package main

import (
	"log"
	"os"
	"crypto/tls"
	"bufio"
	"time"
	"fmt"

	"github.com/IBM/sarama"
	_ "github.com/joho/godotenv/autoload"

	. "kafka-example/common"
	"kafka-example/src/producer"
)



func main() {
	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")

	kafkaConfig := getKafkaConfig(KAFKA_USERNAME, KAFKA_PASSWORD)

	//producers, err := sarama.NewSyncProducer([]string{"kafka:9092"}, kafkaConfig)
    producers, err := sarama.NewSyncProducer([]string{ KAFKA_HOSTNAME }, kafkaConfig)
	if err != nil {
		log.Fatalln("Unable to create kafka producer got error %v", err)
	}
	defer producers.Close()

	log.Println("Success create kafka sync-producer")

	kafka := &producer.KafkaProducer{
		Producer: producers,
	}

	wordList, err := os.OpenFile("words.txt", os.O_RDONLY, 0666)
	if err != nil {
		if os.IsPermission(err) {
			log.Println("Error: Read permission denied.")
		}
	}

	scanner := bufio.NewScanner(wordList)
	scanner.Split(bufio.ScanLines)

	var txtlines []string
	for scanner.Scan() {
		txtlines = append(txtlines, scanner.Text())
	}
	defer wordList.Close()

	// Get topic name.
	topicName := os.Getenv("KAFKA_TOPIC")

	// Test With Number
	// for i := 1; i <= 10; i++ {
	// 	msg := fmt.Sprintf("message number %v", i)
	// 	err := kafka.SendMessage(topicName, msg)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// Using Word List
	id := 1
	for _, eachline := range txtlines {
		result := kafka.SendMessage(topicName, eachline)

		// Block until the result is returned and a server-generated
		// ID is returned for the published message.
		err := result
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("## Send Message [ %v => %s ].\n", id, eachline)
		id = id + 1

		// Test show wordlist
		// fmt.Println(eachline)
	}
}


func getKafkaConfig(username, password string) *sarama.Config {
	
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Retry.Max = 0
	kafkaConfig.Net.WriteTimeout = 5 * time.Second

	if username != "" {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Handshake = true
		kafkaConfig.Net.SASL.User = username
		kafkaConfig.Net.SASL.Password = password
		kafkaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

		tlsConfig := tls.Config{}
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = &tlsConfig
	}

	return kafkaConfig
}


