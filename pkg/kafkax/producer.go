// Package kafkax wraps segmentio/kafka-go with the reliability defaults this
// project argues for: acks=all on the producer and manual offset commits on the
// consumer. kafka-go is a pure-Go client, so the services still build as static
// CGO-free binaries (unlike confluent-kafka-go, which links librdkafka).
package kafkax

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer is a thin JSON producer over a kafka.Writer.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a synchronous producer for a single topic.
//
// RequireAll (acks=all) is chosen deliberately: the leader only acknowledges a
// write after all in-sync replicas have it, so a single broker failure right
// after a post is accepted cannot silently lose the moderation task. The
// alternative acks=1 (leader-only) is faster but drops the message if the
// leader dies before replicating; acks=0 (fire-and-forget) can lose data on any
// hiccup. For a moderation pipeline the correctness cost of a lost task (a post
// stuck invisible forever, or worse, wrongly assumed reviewed) outweighs the
// small latency premium of waiting for replication.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // hash the key so a post_id always maps to one partition
		RequiredAcks: kafka.RequireAll,
		Async:        false, // block until the broker acks, surfacing errors to the caller
		BatchTimeout: 10 * time.Millisecond,
	}
	return &Producer{writer: w}
}

// Publish marshals v to JSON and writes it with the given key. Using a stable
// key (the post_id) means all events for one post land on the same partition
// and are therefore consumed in order.
func (p *Producer) Publish(ctx context.Context, key string, v interface{}) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now(),
	})
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error { return p.writer.Close() }
