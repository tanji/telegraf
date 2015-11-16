package kafka_consumer

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/telegraf/plugins"
	"github.com/wvanbergen/kafka/consumergroup"
)

type Kafka struct {
	ConsumerGroup  string
	Topics         []string
	ZookeeperPeers []string
	Consumer       *consumergroup.ConsumerGroup
	MetricBuffer   int

	sync.Mutex

	// channel for all incoming kafka messages
	in <-chan *sarama.ConsumerMessage
	// channel for all kafka consumer errors
	errs <-chan *sarama.ConsumerError
	// channel for all incoming parsed kafka points
	pointChan chan models.Point
	done      chan struct{}
}

var sampleConfig = `
  # topic(s) to consume
  topics = ["telegraf"]
  # an array of Zookeeper connection strings
  zookeeper_peers = ["localhost:2181"]
  # the name of the consumer group
  consumer_group = "telegraf_metrics_consumers"
  # Maximum number of points to buffer between collection intervals
  metric_buffer = 100000
`

func (k *Kafka) SampleConfig() string {
	return sampleConfig
}

func (k *Kafka) Description() string {
	return "Read line-protocol metrics from Kafka topic(s)"
}

func (k *Kafka) Start() error {
	k.Lock()
	defer k.Unlock()
	var consumerErr error

	if k.Consumer == nil || k.Consumer.Closed() {
		k.Consumer, consumerErr = consumergroup.JoinConsumerGroup(
			k.ConsumerGroup,
			k.Topics,
			k.ZookeeperPeers,
			nil,
		)
		if consumerErr != nil {
			return consumerErr
		}

		// Setup message and error channels
		k.in = k.Consumer.Messages()
		k.errs = k.Consumer.Errors()
	}

	k.done = make(chan struct{})
	k.pointChan = make(chan models.Point, k.MetricBuffer)

	// Start the kafka message reader
	go k.parser()
	return nil
}

// parser() reads all incoming messages from the consumer, and parses them into
// influxdb metric points.
func (k *Kafka) parser() {
	for {
		select {
		case <-k.done:
			return
		case err := <-k.errs:
			log.Printf("Kafka Consumer Error: %s\n", err.Error())
		case msg := <-k.in:
			points, err := models.ParsePoints(msg.Value)
			if err != nil {
				log.Printf("Could not parse kafka message: %s, error: %s",
					string(msg.Value), err.Error())
			}

			for _, point := range points {
				select {
				case k.pointChan <- point:
					continue
				default:
					log.Printf("Kafka Consumer buffer is full, dropping a point." +
						" You may want to increase the message_buffer setting")
				}
			}

			k.Consumer.CommitUpto(msg)
		}
	}
}

func (k *Kafka) Stop() {
	k.Lock()
	defer k.Unlock()
	close(k.done)
	if err := k.Consumer.Close(); err != nil {
		log.Printf("Error closing kafka consumer: %s\n", err.Error())
	}
}

func (k *Kafka) Gather(acc plugins.Accumulator) error {
	k.Lock()
	defer k.Unlock()
	for point := range k.pointChan {
		acc.AddFields(point.Name(), point.Fields(), point.Tags(), point.Time())
	}
	return nil
}

func init() {
	plugins.Add("kafka_consumer", func() plugins.Plugin {
		return &Kafka{}
	})
}
