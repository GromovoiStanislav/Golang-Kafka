## Kafka with github.com/IBM/sarama simple exapmple

Setup:

```
go mod init kafka-example

go get github.com/IBM/sarama
go get github.com/xdg-go/scram

go get github.com/joho/godotenv
```

Start:

```
go run .\consumer-0
go run .\consumer-1
go run .\consumer-all
go run .\consumer-group

go run .\producer
```
