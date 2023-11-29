package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func getMongoCollection(mongoURL, dbName, collectionName string) *mongo.Collection {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB ... !!")

	db := client.Database(dbName)
	collection := db.Collection(collectionName)
	return collection
}



func getKafkaReader(KAFKA_HOSTNAME,KAFKA_USERNAME, KAFKA_PASSWORD, topic, groupID string) *kafka.Reader {

	mechanism, err := scram.Mechanism(scram.SHA256, KAFKA_USERNAME, KAFKA_PASSWORD)
	if err != nil {
		log.Fatalln(err)
	}
	
	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	return  kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{KAFKA_HOSTNAME},
		GroupID:  groupID,
		Topic:    topic,
		Dialer:  dialer,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}


func main() {
	// Загрузите переменные среды из файла .env
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD")
	MONGODB_URI := os.Getenv("MONGODB_URI")

	// get Mongo db Collection using environment variables.
	collection := getMongoCollection(MONGODB_URI, "dbKafka", "collectionName")

	// get kafka reader using environment variables.
	reader := getKafkaReader(KAFKA_HOSTNAME,KAFKA_USERNAME, KAFKA_PASSWORD,"my-topic", "consumer-group-id")
	defer reader.Close()
	
	fmt.Println("start consuming ... !!")

	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}

		insertResult, err := collection.InsertOne(ctx, msg)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	}
}