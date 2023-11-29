## Kafka with github.com/segmentio/kafka-go example consumer-mongo-db

#### Setup:

```
go mod init main
go get github.com/segmentio/kafka-go
go get github.com/segmentio/kafka-go/sasl/scram
go get github.com/joho/godotenv
go get go.mongodb.org/mongo-driver/mongo
```

#### Start:

```
go run .\consumer.go
```
