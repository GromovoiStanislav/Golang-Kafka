package main

import (
	"fmt"
	"os"
	"context"
	"events"
	"crypto/tls"
	"strings"


	"github.com/IBM/sarama"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"github.com/spf13/viper"
	_ "github.com/joho/godotenv/autoload"

	"kafka-exapmple/consumer/repositories"
	"kafka-exapmple/consumer/services"
)

func init() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
}


func initDatabase() *gorm.DB {
	//DSN := os.Getenv("DB_URI")//'root:root@tcp(127.0.0.1:3306)/bank'

	DSN := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
		viper.GetString("db.username"),
		viper.GetString("db.password"),
		viper.GetString("db.host"),
		viper.GetInt("db.port"),
		viper.GetString("db.database"),
	)


	db, err := gorm.Open(mysql.Open(DSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic(err)
	}
	return db
}



func main() {

	KAFKA_HOSTNAME := os.Getenv("KAFKA_HOSTNAME") // viper.GetString("kafka.hostname")
	KAFKA_USERNAME := os.Getenv("KAFKA_USERNAME") // viper.GetString("kafka.username")
	KAFKA_PASSWORD := os.Getenv("KAFKA_PASSWORD") // viper.GetString("kafka.password")

	brokers := []string{KAFKA_HOSTNAME}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = KAFKA_USERNAME
	config.Net.SASL.Password = KAFKA_PASSWORD
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256

	tlsConfig := tls.Config{}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tlsConfig

	consumer, err := sarama.NewConsumerGroup(brokers, viper.GetString("kafka.group"), config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()


	db := initDatabase()
	accountRepo := repositories.NewAccountRepository(db)
	accountEventHandler := services.NewAccountEventHandler(accountRepo)
	accountConsumerHandler := services.NewConsumerHandler(accountEventHandler)

	fmt.Println("Account consumer started...")
	for {
		consumer.Consume(context.Background(), events.Topics, accountConsumerHandler)
	}
}
