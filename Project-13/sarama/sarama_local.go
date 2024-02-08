package sarama

type SaramaLocal struct {
	Brokers []string
	Username string
	Password string
	Topic   string
}

func NewSaramaLocal(brokers []string, username string, password string, topic string) SaramaLocal {
	return SaramaLocal{
		Brokers: brokers,
		Username: username,
		Password: password,
		Topic:   topic,
	}
}