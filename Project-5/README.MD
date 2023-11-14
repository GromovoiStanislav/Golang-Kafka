## Kafka with github.com/segmentio/kafka-go Bridge

Setup:

```
go mod init kafka-go
go get github.com/segmentio/kafka-go
go get github.com/segmentio/kafka-go/sasl/scram
go get github.com/joho/godotenv
go get golang.org/x/sync/errgroup
```

Start:

```
go run .\main.go
go run .\consumer.go
go run .\producer.go
```
