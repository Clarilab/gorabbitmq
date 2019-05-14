package gorabbitmq

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

// QueueConnector is the tcp connection for the communication with the RabbitMQ Server
// can be used to connect to a queue
type QueueConnector interface {
	ConnectToQueue(settings QueueSettings) (Queue, error)
}

type queueConnector struct {
	QueueConnector
	connection *amqp.Connection
	channel    *channel
}

type channel struct {
	channel             *amqp.Channel
	channelErrorChannel chan *amqp.Error
	closed              bool
}

func getConnectionString(queueSettings ConnectionSettings) string {
	var sb strings.Builder

	sb.WriteString("amqp://")
	sb.WriteString(queueSettings.UserName)
	sb.WriteString(":")
	sb.WriteString(queueSettings.Password)
	sb.WriteString("@")
	sb.WriteString(queueSettings.Host)
	sb.WriteString(":")
	sb.WriteString(strconv.Itoa(queueSettings.Port))
	sb.WriteString("/")

	return sb.String()
}

// NewConnection returns a new Instance of a tcp Connection to a RabbitMQ Server
func NewConnection(settings ConnectionSettings) (QueueConnector, error) {
	connectionString := getConnectionString(settings)

	conn, err := amqp.Dial(connectionString)

	if err != nil {
		return nil, err
	}

	connector := &queueConnector{
		connection: conn,
	}

	connector.createChannel()

	return connector, err
}

func (c *queueConnector) watchChannelConnection() {
	for elem := range c.channel.channelErrorChannel {
		if c.channel.closed {
			continue
		}

		fmt.Println(elem)
		c.createChannel()
	}
}

func (c *queueConnector) watchChannelClosed() {

}

func (c *queueConnector) createChannel() error {
	ch, err := c.connection.Channel()

	channelErrorChannel := make(chan *amqp.Error)

	if c.channel != nil && !c.channel.closed {
		close(c.channel.channelErrorChannel)
	}

	c.channel = &channel{
		channel:             ch,
		channelErrorChannel: channelErrorChannel,
		closed:              false,
	}

	if err != nil {
		return err
	}

	go c.watchChannelConnection()
	go c.watchChannelClosed()

	ch.NotifyClose(channelErrorChannel)

	return nil
}

func (c *channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (c *channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *channel) close() {
	c.closed = true
	c.channel.Close()
}

// ConnectToChannel connects to a channel
func (c *queueConnector) ConnectToQueue(queueSettings QueueSettings) (Queue, error) {
	if c.channel == nil || c.channel.closed {
		err := c.createChannel()

		if err != nil {
			return nil, err
		}
	}

	nativeQueue, err := c.channel.channel.QueueDeclare(
		queueSettings.QueueName,
		queueSettings.Durable,
		queueSettings.DeleteWhenUnused,
		queueSettings.Exclusive,
		queueSettings.NoWait,
		nil,
	)

	if err != nil {
		return nil, err
	}

	connection := &queue{
		queueSettings: queueSettings,
		channel:       c.channel,
		queue:         nativeQueue,
	}

	return connection, nil
}
