## Kafka and Avro with github.com/segmentio/kafka-go simple exapmple

#### Setup:

```
go mod init main
go get github.com/segmentio/kafka-go
go get github.com/segmentio/kafka-go/sasl/scram
go get github.com/joho/godotenv
github.com/linkedin/goavro/v2
```

#### Start:

```
go run .\consumer.go
go run .\producer.go
```
