package gorabbitmq

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	publish string = "publish"
	consume string = "consume"

	closeWGDelta int = 2

	reconnectFailChanSize int = 16
)

// Connector manages the connection to a RabbitMQ cluster.
type Connector struct {
	options *ConnectorOptions
	log     *log

	publishConnection *connection
	consumeConnection *connection
}

// NewConnector creates a new Connector instance.
//
// Needs to be closed with the Close() method.
func NewConnector(settings *ConnectionSettings, options ...ConnectorOption) *Connector {
	opt := defaultConnectorOptions(
		fmt.Sprintf("amqp://%s:%s@%s/",
			url.QueryEscape(settings.UserName),
			url.QueryEscape(settings.Password),
			net.JoinHostPort(
				url.QueryEscape(settings.Host),
				strconv.Itoa(settings.Port),
			),
		),
	)

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	return &Connector{
		options: opt,
		log:     newLogger(opt.logger),
	}
}

// Close gracefully closes the connection to the server.
func (c *Connector) Close() error {
	const errMessage = "failed to close connections to rabbitmq gracefully: %w"

	if c.publishConnection != nil &&
		c.publishConnection.amqpConnection != nil {
		c.log.logDebug("closing connection", "type", publish)

		c.publishConnection.connectionCloseWG.Add(closeWGDelta)

		if err := c.publishConnection.amqpConnection.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.publishConnection.connectionCloseWG.Wait()
	}

	if c.consumeConnection != nil &&
		c.consumeConnection.amqpConnection != nil {
		c.log.logDebug("closing connection", "type", consume)

		c.consumeConnection.connectionCloseWG.Add(closeWGDelta)

		if err := c.consumeConnection.amqpConnection.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.consumeConnection.connectionCloseWG.Wait()
	}

	c.log.logInfo("gracefully closed connections to rabbitmq")

	return nil
}

// DecodeDeliveryBody can be used to decode the body of a delivery into v.
func (c *Connector) DecodeDeliveryBody(delivery Delivery, v any) error {
	const errMessage = "failed to decode delivery body: %w"

	if err := c.options.Codec.Decoder(delivery.Body, v); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Connector) NotifyPublishRecoveryFailed() <-chan ReconnectionFailError {
	return c.publishConnection.reconnectFailChan
}

func (c *Connector) NotifyConsumeRecoveryFailed() <-chan ReconnectionFailError {
	return c.publishConnection.reconnectFailChan
}

func connect(conn *connection, opt *ConnectorOptions, logger *log, instanceType string) error {
	const errMessage = "failed to connect to rabbitmq: %w"

	if conn.amqpConnection == nil {
		err := createConnection(conn, opt, instanceType, logger)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		err = createChannel(conn, opt, instanceType, logger)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		conn.reconnectChan = make(chan struct{})

		conn.watchReconnects(instanceType, opt, logger)
		conn.watchConsumerClose()
	}

	return nil
}

func createConnection(conn *connection, opt *ConnectorOptions, instanceType string, logger *log) error {
	const errMessage = "failed to create channel: %w"

	var err error

	conn.amqpConnection, err = amqp.DialConfig(opt.uri, amqp.Config(*opt.Config))
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	watchConnectionNotifications(conn, instanceType, logger)

	return nil
}

func createChannel(conn *connection, opt *ConnectorOptions, instanceType string, logger *log) error {
	const errMessage = "failed to create channel: %w"

	var err error

	conn.amqpChannel, err = conn.amqpConnection.Channel()
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if opt.PrefetchCount > 0 {
		err = conn.amqpChannel.Qos(opt.PrefetchCount, 0, false)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	watchChannelNotifications(conn, instanceType, opt.ReturnHandler, logger)

	return nil
}

func watchConnectionNotifications(conn *connection, instanceType string, logger *log) {
	closeChan := conn.amqpConnection.NotifyClose(make(chan *amqp.Error))
	blockChan := conn.amqpConnection.NotifyBlocked(make(chan amqp.Blocking))

	go func() {
		for {
			select {
			case err := <-closeChan:
				if err == nil {
					slog.Debug("closed connection", "type", instanceType)

					conn.connectionCloseWG.Done()

					return
				}

				logger.logDebug("connection unexpectedly closed", "type", instanceType, "cause", err)

				conn.reconnectChan <- struct{}{}

				return

			case block := <-blockChan:
				logger.logWarn("connection exception", "type", instanceType, "cause", block.Reason)
			}
		}
	}()
}

func watchChannelNotifications(conn *connection, instanceType string, returnHandler ReturnHandler, logger *log) {
	closeChan := conn.amqpChannel.NotifyClose(make(chan *amqp.Error))
	cancelChan := conn.amqpChannel.NotifyCancel(make(chan string))
	returnChan := conn.amqpChannel.NotifyReturn(make(chan amqp.Return))

	go func() {
		for {
			select {
			case err := <-closeChan:
				if err == nil {
					slog.Debug("closed channel", "type", instanceType)

					conn.connectionCloseWG.Done()

					return
				}

				logger.logDebug("channel unexpectedly closed", "type", instanceType, "cause", err)

				return

			case tag := <-cancelChan:
				logger.logWarn("cancel exception", "type", instanceType, "cause", tag)

			case rtrn := <-returnChan:
				if returnHandler != nil {
					returnHandler(Return(rtrn))

					continue
				}

				logger.logWarn(
					"message could not be published",
					"replyCode", rtrn.ReplyCode,
					"replyText", rtrn.ReplyText,
					"messageId", rtrn.MessageId,
					"correlationId", rtrn.CorrelationId,
					"exchange", rtrn.Exchange,
					"routingKey", rtrn.RoutingKey,
				)
			}
		}
	}()
}
