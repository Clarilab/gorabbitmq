package gorabbitmq

import (
	"context"
	"fmt"
	"time"
)

const (
	ArgDLX string = "x-dead-letter-exchange"
	ArgDLK string = "x-dead-letter-routing-key"
	ArgTTL string = "x-message-ttl"

	keyRetryCount string = "x-retry-count"
	requeueSuffix string = "_requeue"
	dlxPrefix     string = "dlx_"
)

func defaultDLXOptions(dlxName string) *ExchangeOptions {
	return &ExchangeOptions{
		Declare:    true,
		Name:       dlxName,
		Kind:       ExchangeDirect,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Passive:    false,
		Args:       nil,
	}
}

func defaultDLQOptions(name, dlxName, routingKey string, ttl time.Duration) *QueueOptions {
	return &QueueOptions{
		Declare:    true,
		name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Passive:    false,
		Args: map[string]any{
			ArgDLX: dlxName, // original exchange, in which the event gets retried
			ArgDLK: routingKey,
			ArgTTL: ttl.Milliseconds(),
		},
	}
}

func (c *Consumer) setupDeadLetterRetry() error {
	const errMessage = "failed to setup dead letter exchange retry %w"

	routingKey := c.options.QueueOptions.name + requeueSuffix

	c.options.dlxRetryOptions.dlxName = dlxPrefix + c.options.ExchangeOptions.Name
	c.options.dlxRetryOptions.dlqName = dlxPrefix + c.options.QueueOptions.name

	err := declareExchange(c.conn.amqpChannel, defaultDLXOptions(c.options.dlxRetryOptions.dlxName))
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	bindings := make([]Binding, 0, len(c.options.dlxRetryOptions.delays)+1)

	// declare and bind queues with ttl values
	for _, ttl := range c.options.dlxRetryOptions.delays {
		queueName := fmt.Sprintf("%s_%s", c.options.dlxRetryOptions.dlqName, ttl.String())

		err = declareQueue(c.conn.amqpChannel, defaultDLQOptions(queueName, c.options.ExchangeOptions.Name, routingKey, ttl))
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		bindings = append(bindings, Binding{
			BindingOptions: defaultBindingOptions(),
			RoutingKey:     queueName,
			QueueName:      queueName,
			ExchangeName:   c.options.dlxRetryOptions.dlxName,
		})
	}

	// append binding for the original queue
	bindings = append(bindings,
		Binding{
			BindingOptions: defaultBindingOptions(),
			RoutingKey:     routingKey,
			ExchangeName:   c.options.ExchangeOptions.Name,
			QueueName:      c.options.QueueOptions.name,
		},
	)

	err = declareBindings(c.conn.amqpChannel, "", "", bindings)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Consumer) handleDeadLetterMessage(
	delivery *Delivery,
) (Action, error) {
	const errMessage = "failed to handle dead letter exchange message %w"

	retryCount, ok := delivery.Headers[keyRetryCount].(int32)
	if !ok {
		retryCount = 0
	}

	// drop the event after max retries exceeded
	if int64(retryCount) >= c.options.dlxRetryOptions.maxRetries {
		return NackDiscard, nil
	}

	// if retryCount exceeds number of delays, use the last defined delay value
	ttl := c.options.dlxRetryOptions.delays[min(int(retryCount), len(c.options.dlxRetryOptions.delays)-1)]

	if delivery.Headers == nil {
		delivery.Headers = make(map[string]any)
	}

	delivery.Headers[keyRetryCount] = retryCount + 1

	err := c.options.dlxRetryOptions.dlxPublisher.PublishWithOptions(
		context.Background(),
		[]string{fmt.Sprintf("%s_%s", c.options.dlxRetryOptions.dlqName, ttl.String())},
		delivery.Body,
		WithPublishOptionMandatory(true),
		WithPublishOptionDeliveryMode(PersistentDelivery),
		WithPublishOptionExchange(c.options.dlxRetryOptions.dlxName),
		WithPublishOptionHeaders(Table(delivery.Headers)),
	)
	if err != nil {
		return NackRequeue, fmt.Errorf(errMessage, err)
	}

	return Ack, nil
}
