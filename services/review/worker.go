package review

import (
	"context"
	"encoding/json"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/event"
	"github.com/WindyRivers/moderate-pipe/internal/model"
	"github.com/WindyRivers/moderate-pipe/pkg/kafkax"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"github.com/WindyRivers/moderate-pipe/services/user"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Worker is one Review Service consumer loop. Several Workers across processes
// share a Kafka consumer group, so partitions are balanced across them and a
// crashed Worker's partitions are reassigned automatically.
type Worker struct {
	consumer   *kafkax.Consumer
	resultProd *kafkax.Producer
	dlqProd    *kafkax.Producer
	repo       *Repo
	userRepo   *user.Repo // shared-DB access for reputation write-back on rejection
	userClient *UserClient
	engine     *Engine
	instanceID string
}

func NewWorker(
	consumer *kafkax.Consumer,
	resultProd, dlqProd *kafkax.Producer,
	repo *Repo, userRepo *user.Repo,
	userClient *UserClient, engine *Engine, instanceID string,
) *Worker {
	return &Worker{
		consumer: consumer, resultProd: resultProd, dlqProd: dlqProd,
		repo: repo, userRepo: userRepo, userClient: userClient,
		engine: engine, instanceID: instanceID,
	}
}

// Run consumes until ctx is cancelled. The loop is: fetch (no commit) → process
// → commit. The offset is committed only after processing durably succeeds (or
// the message is parked in the DLQ), which is what makes delivery at-least-once
// rather than at-most-once.
func (w *Worker) Run(ctx context.Context) {
	log := logger.L().With(zap.String("instance", w.instanceID))
	log.Info("review worker started")
	for {
		msg, err := w.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("review worker stopping")
				return
			}
			log.Warn("fetch message", zap.Error(err))
			continue
		}
		w.handle(ctx, log, msg)
	}
}

func (w *Worker) handle(ctx context.Context, log *zap.Logger, msg kafka.Message) {
	var task event.ReviewTask
	if err := json.Unmarshal(msg.Value, &task); err != nil {
		// Poison message: it will never parse, so retrying is pointless. Park it
		// in the DLQ and commit so it doesn't wedge the partition forever.
		log.Error("unparseable task -> DLQ", zap.Error(err))
		w.toDLQ(ctx, log, msg, "unmarshal_error")
		w.commit(ctx, log, msg)
		return
	}
	log = log.With(zap.Uint("post_id", task.PostID),
		zap.Int("partition", msg.Partition), zap.Int64("offset", msg.Offset))

	// Idempotency pre-check: if we already have a result for this post, this is
	// a redelivery — skip straight to commit without re-moderating or
	// re-penalising the user.
	if exists, err := w.repo.Exists(ctx, task.PostID); err == nil && exists {
		log.Info("duplicate delivery, already moderated — skipping")
		w.commit(ctx, log, msg)
		return
	}

	// Reputation lookup: fault-tolerant, returns degraded=true on outage.
	reputation, degraded := w.userClient.GetReputation(ctx, task.UserID)

	decision := w.engine.Evaluate(task, reputation, degraded)

	// Persist + fan out, with retries. If this fails repeatedly the message is
	// dead-lettered rather than blocking the partition.
	if err := w.persistWithRetry(ctx, log, task, decision, degraded); err != nil {
		log.Error("processing failed after retries -> DLQ", zap.Error(err))
		w.toDLQ(ctx, log, msg, "processing_error")
		w.commit(ctx, log, msg)
		return
	}

	// End-to-end latency from post submission to moderation decision — the
	// number the acceptance criteria asks us to capture in monitoring logs.
	latency := time.Since(task.SubmittedAt)
	log.Info("moderation complete",
		zap.String("status", string(decision.Status)),
		zap.String("reason", decision.Reason),
		zap.String("matched_word", decision.MatchedWord),
		zap.Bool("degraded", degraded),
		zap.Int("reputation", reputation),
		zap.Duration("e2e_latency", latency))

	w.commit(ctx, log, msg)
}

// persistWithRetry writes the moderation result idempotently, applies the
// reputation penalty on rejection, and publishes the result event. The whole
// unit retries with exponential backoff so a transient DB/broker hiccup
// self-heals instead of dead-lettering.
func (w *Worker) persistWithRetry(ctx context.Context, log *zap.Logger, task event.ReviewTask, d Decision, degraded bool) error {
	const attempts = 3
	backoff := 100 * time.Millisecond
	var lastErr error
	for i := 0; i < attempts; i++ {
		err := w.persistOnce(ctx, task, d, degraded)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Warn("persist attempt failed, retrying",
			zap.Int("attempt", i+1), zap.Error(err))
		if i < attempts-1 {
			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

func (w *Worker) persistOnce(ctx context.Context, task event.ReviewTask, d Decision, degraded bool) error {
	result := &model.ModerationResult{
		PostID:      task.PostID,
		UserID:      task.UserID,
		Status:      d.Status,
		Reason:      d.Reason,
		MatchedWord: d.MatchedWord,
		Degraded:    degraded,
	}
	created, err := w.repo.SaveResultIfAbsent(ctx, result)
	if err != nil {
		return err
	}
	// Only apply side effects the first time we create the result, so a retry
	// after a partial failure can't double-penalise the user.
	if created && d.Status == model.StatusRejected {
		if err := w.userRepo.ApplyViolation(ctx, task.UserID); err != nil {
			return err
		}
	}

	// Notify the Content Service of the decision.
	return w.resultProd.Publish(ctx, itoa(task.PostID), event.ReviewResult{
		PostID:     task.PostID,
		UserID:     task.UserID,
		Status:     string(d.Status),
		Reason:     d.Reason,
		Degraded:   degraded,
		ReviewedAt: time.Now(),
	})
}

// toDLQ forwards the raw message to the dead-letter topic with a failure-reason
// header, then logs an alert-worthy line.
func (w *Worker) toDLQ(ctx context.Context, log *zap.Logger, msg kafka.Message, reason string) {
	payload := map[string]interface{}{
		"reason":          reason,
		"original_topic":  msg.Topic,
		"partition":       msg.Partition,
		"offset":          msg.Offset,
		"key":             string(msg.Key),
		"value":           json.RawMessage(msg.Value),
		"dead_lettered_at": time.Now(),
	}
	if err := w.dlqProd.Publish(ctx, string(msg.Key), payload); err != nil {
		log.Error("failed to write to DLQ", zap.Error(err))
		return
	}
	log.Error("ALERT: message dead-lettered", zap.String("reason", reason))
}

func (w *Worker) commit(ctx context.Context, log *zap.Logger, msg kafka.Message) {
	if err := w.consumer.Commit(ctx, msg); err != nil {
		log.Warn("commit offset", zap.Error(err))
	}
}

func itoa(u uint) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
