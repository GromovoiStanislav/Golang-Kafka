## Kafka with github.com/IBM/sarama with Fiber & Gorm (mysql)

Setup:

```
go mod init kafka-exapmple

go get github.com/IBM/sarama
go get github.com/xdg-go/scram

go get github.com/joho/godotenv
go get github.com/spf13/viper
go get github.com/gofiber/fiber/v2
go get gorm.io/driver/mysql
go get gorm.io/gorm
```

Start:

```
go run .\consumer\
go run .\producer
```
