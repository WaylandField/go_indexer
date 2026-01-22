package telemetry

import (
	"context"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

type kafkaHeaderCarrier struct {
	headers []kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, header := range c.headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, value string) {
	for i := range c.headers {
		if strings.EqualFold(c.headers[i].Key, key) {
			c.headers[i].Value = []byte(value)
			return
		}
	}
	c.headers = append(c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.headers))
	for _, header := range c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

func InjectKafkaHeaders(ctx context.Context, headers *[]kafka.Header) {
	carrier := kafkaHeaderCarrier{headers: *headers}
	otel.GetTextMapPropagator().Inject(ctx, &carrier)
	*headers = carrier.headers
}

func ExtractKafkaHeaders(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := kafkaHeaderCarrier{headers: headers}
	return otel.GetTextMapPropagator().Extract(ctx, &carrier)
}
