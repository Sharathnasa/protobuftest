package main

import (
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"os/signal"
	example "protobuftest/example"
	"syscall"
	"time"
)

func main() {
	topic := "yoyoyoyo"
	brokerList := []string{"localhost:9092"}

	producer, err := newSyncProducer(brokerList)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {

			case t := <-ticker.C:
				elliot := &example.GenericMessage{
					DescriptorSet: nil,
					Message:       nil,
				}
				pixelToSend := elliot
				pixelToSendBytes, err := proto.Marshal(pixelToSend)
				if err != nil {
					log.Fatalln("Failed to marshal example:", err)
				}

				msg := &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(pixelToSendBytes),
				}

				producer.SendMessage(msg)
				log.Printf("Pixel sent: %s", pixelToSend)
			}
		}

	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	partitionConsumer, err := newPartitionConsumer(brokerList, topic)
	if err != nil {
		log.Fatalln("Failed to create Sarama partition consumer:", err)
	}

	log.Println("Waiting for messages...")

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			receivedPixel := &example.Pixel{}
			err := proto.Unmarshal(msg.Value, receivedPixel)
			if err != nil {
				log.Fatalln("Failed to unmarshal example:", err)
			}

			log.Printf("Pixel received: %s", receivedPixel)
		case <-signals:
			log.Print("Received termination signal. Exiting.")
			return
		}
	}
}

func newSyncProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	// TODO configure producer

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func newPartitionConsumer(brokerList []string, topic string) (sarama.PartitionConsumer, error) {
	conf := sarama.NewConfig()
	// TODO configure consumer
	consumer, err := sarama.NewConsumer(brokerList, conf)
	if err != nil {
		return nil, err
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	return partitionConsumer, err
}
