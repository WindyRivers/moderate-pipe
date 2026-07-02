package kafkax

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates the topic with the requested partition count if it does
// not already exist. Auto-topic-creation is often disabled on real clusters and
// always creates single-partition topics, which would defeat the consumer-group
// parallelism this project demonstrates — so we create topics explicitly with
// the partition count we want.
//
// The partition count is the ceiling on consumer-group parallelism: at most one
// consumer in a group reads a given partition, so N partitions can be served by
// at most N active consumers (extra consumers sit idle as hot standbys). We
// default the review topic to 3 partitions so up to 3 Review Service instances
// share the load.
func EnsureTopic(brokers []string, topic string, partitions, replicationFactor int) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}
	// Dial any broker, then dial the cluster controller to run admin ops.
	conn, err := dialWithRetry(brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := dialWithRetry(net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	// Creating a topic that already exists is fine and expected when several
	// services call EnsureTopic on startup — treat it as success.
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

// dialWithRetry tolerates a broker that is still coming up in the compose stack.
func dialWithRetry(addr string) (*kafka.Conn, error) {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := kafka.DialContext(context.Background(), "tcp", addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	return nil, lastErr
}
