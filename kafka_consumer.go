package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct {
	n     int
	delay time.Duration
	count int
}

func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("MemberID: %s, GenerationID: %d, Topic: %s, Partition: %d\n", sess.MemberID(), sess.GenerationID(), claim.Topic(), claim.Partition())
	for {
		select {
		case msg := <-claim.Messages():
			log.Printf("Получено сообщение %d: %s\n", h.count+1, string(msg.Value))
			sess.MarkMessage(msg, "")
			h.count++
		case <-sess.Context().Done():
			return nil
		}
		time.Sleep(h.delay)
	}
}

func main() {
	// Параметры командной строки
	var topic string
	var n int
	var delay int
	var group string

	flag.StringVar(&topic, "topic", "your_topic", "Имя топика")
	flag.IntVar(&n, "n", 10, "Количество сообщений для чтения")
	flag.IntVar(&delay, "delay", 1000, "Задержка между чтением сообщений в миллисекундах")
	flag.StringVar(&group, "group", "your_group", "Имя группы потребителей")
	flag.Parse()

	// Чтение переменных окружения
	brokerList := os.Getenv("KAFKA_BROKERS")
	if brokerList == "" {
		brokerList = "localhost:9092" // Значение по умолчанию
	}

	// Настройки клиента
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 1 * time.Second
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Создание клиента
	client, err := sarama.NewConsumerGroup(strings.Split(brokerList, ","), group, config)
	if err != nil {
		log.Fatalf("Ошибка при создании клиента: %v", err)
	}
	defer client.Close()

	log.Printf("Потребитель %s начал работу", group)

	handler := &ConsumerGroupHandler{
		n:     n,
		delay: time.Duration(delay) * time.Millisecond,
	}

	// Запуск потребителя
	ctx := context.Background()
	for {
		if err := client.Consume(ctx, []string{topic}, handler); err != nil {
			log.Fatalf("Ошибка при потреблении: %v", err)
		}
	}
}
