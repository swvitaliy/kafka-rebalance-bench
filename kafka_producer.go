package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "math/rand"
    "os"
    "strings"
    "time"

    "github.com/IBM/sarama"
)

// Генерация случайного сообщения с ключом
func generateRandomMessage() (string, map[string]string) {
    key := fmt.Sprintf("key%d", rand.Intn(1000))
    value := map[string]string{
        "key":   key,
        "value": fmt.Sprintf("value%d", rand.Intn(1000)),
    }
    return key, value
}

func main() {
    // Задание количества сообщений и задержки через параметры
    var N int
    var delay int
    flag.IntVar(&N, "n", 10, "Количество сообщений для отправки")
    flag.IntVar(&delay, "d", 100, "Задержка между сообщениями (в миллисекундах)")
    flag.Parse()

    // Получение параметров соединения с Kafka из переменных окружения
    brokersEnv := os.Getenv("KAFKA_BROKERS")
    if brokersEnv == "" {
        brokersEnv = "localhost:9092" // Значение по умолчанию
    }
    brokers := strings.Split(brokersEnv, ",")

    topic := os.Getenv("KAFKA_TOPIC")
    if topic == "" {
        topic = "your_topic" // Значение по умолчанию
    }

    // Конфигурация клиента Kafka
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Producer.Partitioner = sarama.NewRandomPartitioner

    // Создание продюсера
    producer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        log.Fatalf("Ошибка создания продюсера: %v", err)
    }
    defer func() {
        if err := producer.Close(); err != nil {
            log.Fatalf("Ошибка закрытия продюсера: %v", err)
        }
    }()

    for i := 0; i < N; i++ {
        // Генерация случайного сообщения с ключом
        key, message := generateRandomMessage()

        // Сериализация сообщения в JSON
        messageBytes, err := json.Marshal(message)
        if err != nil {
            log.Fatalf("Ошибка сериализации сообщения: %v", err)
        }

        // Создание Kafka-сообщения
        msg := &sarama.ProducerMessage{
            Topic: topic,
            Key:   sarama.StringEncoder(key),
            Value: sarama.ByteEncoder(messageBytes),
        }

        // Отправка сообщения
        partition, offset, err := producer.SendMessage(msg)
        if err != nil {
            log.Fatalf("Ошибка отправки сообщения: %v", err)
        }

        fmt.Printf("Сообщение %d с ключом %s отправлено в партицию %d с оффсетом %d\n", i+1, key, partition, offset)

        // Задержка
        time.Sleep(time.Duration(delay) * time.Millisecond)
    }
}
