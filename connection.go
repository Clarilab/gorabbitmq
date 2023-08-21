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

			backoff(
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
				logger,
			)

			c.recoverPublishers(logger)

			err := c.recoverConsumers(logger)
			if err != nil {
				logger.logFatal("reconnection failed", "error", err)
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

		logger.logDebug("successfully recovered publishers(s)", "publisherCount", len(c.publishers))

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
	initDelay  time.Duration
	factor     int
	maxRetries int
}

func backoff(action backoffAction, params *backoffParams, logger *log) {
	retry := 0

	for retry <= params.maxRetries {
		if action() == nil {
			logger.logDebug("successfully reestablished connection to rabbitmq", "retries", retry)

			break
		}

		if retry == params.maxRetries {
			logger.logFatal("reconnection to rabbitmq failed! maximum retries exceeded", "retries", retry)
		}

		delay := time.Duration(params.factor*retry) * params.initDelay

		logger.logDebug("failed to reconnect to rabbitmq: backing off...", "backoff-time", delay.String())

		time.Sleep(delay)

		retry++
	}
}

type backoffAction func() error
