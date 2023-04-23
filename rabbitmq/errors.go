package rabbitmq

import "errors"

var (
	ConnIsNil            = errors.New("conn is nil")
	ExchangeNameIsEmpty  = errors.New("exchange name is empty")
	QueueNameIsEmpty     = errors.New("queue name is empty")
	RoutingKeyIsRequired = errors.New("routingKey is required")
)
