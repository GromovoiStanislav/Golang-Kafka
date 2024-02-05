## Kafka with github.com/IBM/sarama simple exapmple

Setup:

```
go mod init kafka-exapmple

go get github.com/IBM/sarama
go get github.com/xdg-go/scram

go get github.com/joho/godotenv
```

Start:

```
go run .\consumer.go
go run .\producer.go

go run .\main.go
```
