## Kafka with github.com/segmentio/kafka-go writing to multiple topics

Setup:

```
go mod init main
go get github.com/segmentio/kafka-go
go get github.com/segmentio/kafka-go/sasl/scram
go get github.com/joho/godotenv
```

Start:

```
go run .\consumer.go -topic=topic-A
go run .\consumer.go -topic=topic-B
go run .\consumer.go -topic=topic-C
go run .\producer.go
```
