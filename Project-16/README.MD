## Kafka with github.com/IBM/sarama exapmple

#### Setup:

```
go mod init kafka-example

go get github.com/IBM/sarama
go get github.com/xdg-go/scram

go get github.com/joho/godotenv
```

#### Start:

```
go run .\consumer-group
go run .\producer
```
