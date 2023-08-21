package gorabbitmq

import (
	"fmt"
	"maps"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type connection struct {
	amqpConnection    *amqp.Connection
	amqpChannel       *amqp.Channel
	connectionCloseWG *sync.WaitGroup
	reconnectChan     chan struct{}
	reconnectFailChan chan ReconnectionFailError
	consumerCloseChan chan string
	consumersMtx      *sync.Mutex
	consumers         map[string]*Consumer
	publishersMtx     *sync.Mutex
	publishers        map[string]*Publisher
}

func (c *connection) watchConsumerClose() {
	go func() {
		for consumer := range c.consumerCloseChan {
			c.consumersMtx.Lock()

			maps.DeleteFunc(c.consumers, func(k string, v *Consumer) bool {
				return consumer == k
			})

			c.consumersMtx.Unlock()
		}
	}()
}

func (c *connection) watchReconnects(instanceType string, opt *ConnectorOptions, logger *log) {
	go func() {
		for range c.reconnectChan {
			c.amqpConnection = nil
			c.amqpChannel = nil

			err := backoff(
				func() error {
					err := createConnection(c, opt, instanceType, logger)
					if err != nil {
						return err
					}

					err = createChannel(c, opt, instanceType, logger)
					if err != nil {
						return err
					}

					return nil
				},
				&backoffParams{
					initDelay:  opt.ReconnectInterval,
					maxRetries: opt.MaxReconnectRetries,
					factor:     opt.BackoffFactor,
				},
				c.reconnectFailChan,
				logger,
			)
			if err != nil {
				break
			}

			c.recoverPublishers(logger)

			err = c.recoverConsumers(logger)
			if err != nil {
				c.reconnectFailChan <- ReconnectionFailError{msg: fmt.Sprintf("reconnection failed: %s", err.Error())}

				logger.logDebug("reconnection failed: maximum retries exceeded", "")

				break
			}

			logger.logInfo(fmt.Sprintf("successfully reconnected %s connection", instanceType))
		}
	}()
}

func (c *connection) recoverPublishers(logger *log) {
	if c.publishers != nil {
		c.publishersMtx.Lock()

		for i := range c.publishers {
			publisher := c.publishers[i]

			publisher.channel = c.amqpChannel
		}

		logger.logDebug("successfully recovered publisher(s)", "publisherCount", len(c.publishers))

		c.publishersMtx.Unlock()
	}
}

func (c *connection) recoverConsumers(logger *log) error {
	const errMessage = "failed to recover consumers %w"

	if c.consumers != nil {
		c.consumersMtx.Lock()

		for i := range c.consumers {
			consumer := c.consumers[i]

			consumer.channel = c.amqpChannel

			err := consumer.startConsuming()
			if err != nil {
				return fmt.Errorf(errMessage, err)
			}
		}

		logger.logDebug("successfully recovered consumer(s)", "consumerCount", len(c.consumers))

		c.consumersMtx.Unlock()
	}

	return nil
}

type backoffParams struct {
	instanceType string
	initDelay    time.Duration
	factor       int
	maxRetries   int
}

type ReconnectionFailError struct {
	msg string
}

func (e ReconnectionFailError) Error() string {
	return e.msg
}

func backoff(action backoffAction, params *backoffParams, failChan chan<- ReconnectionFailError, logger *log) error {
	retry := 0

	for retry <= params.maxRetries {
		if action() == nil {
			logger.logDebug(fmt.Sprintf("successfully reestablished %s connection", params.instanceType), "retries", retry)

			break
		}

		if retry == params.maxRetries {
			err := ReconnectionFailError{msg: fmt.Sprintf("%s reconnection failed: maximum retries exceeded", params.instanceType)}

			failChan <- err

			logger.logDebug(fmt.Sprintf("%s reconnection failed: maximum retries exceeded", params.instanceType), "retries", retry)

			return err
		}

		delay := time.Duration(params.factor*retry) * params.initDelay

		logger.logDebug("failed to reconnect: backing off...", "backoff-time", delay.String())

		time.Sleep(delay)

		retry++
	}

	return nil
}

type backoffAction func() error
