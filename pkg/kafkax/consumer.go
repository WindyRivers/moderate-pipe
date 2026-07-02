package kafkax

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Consumer is a consumer-group reader with manual offset commits.
//
// Auto-commit is deliberately disabled (CommitInterval left at 0). With
// auto-commit, kafka-go would periodically advance the offset on a timer
// regardless of whether the message was actually processed — a crash between
// the commit and the processing would silently drop the message ("at-most-once"
// with a data-loss window). Instead the caller commits *after* it has durably
// written the moderation result, which yields at-least-once delivery: a crash
// before commit simply redelivers the message, and the idempotent consumer
// (unique index on post_id) absorbs the duplicate.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer joins the given consumer group on a topic. Multiple Review
// Service instances that share groupID form one group; Kafka assigns each
// partition to exactly one member, giving load-balanced consumption and
// automatic rebalancing when a member joins or dies.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10MB
		CommitInterval: 0,        // 0 => commits only happen when we call CommitMessages
		StartOffset:    kafka.FirstOffset,
	})
	return &Consumer{reader: r}
}

// Fetch returns the next message without committing its offset. Blocks until a
// message is available or ctx is cancelled.
func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// Commit advances the committed offset past msg. Call only after the message
// has been fully and durably processed.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

// Close leaves the group and closes the reader.
func (c *Consumer) Close() error { return c.reader.Close() }
