package content

import (
	"context"
	"encoding/json"

	"github.com/WindyRivers/moderate-pipe/internal/event"
	"github.com/WindyRivers/moderate-pipe/pkg/kafkax"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"go.uber.org/zap"
)

// ResultConsumer runs inside the Content Service and consumes
// review-result-topic, applying each decision to MySQL and the status cache.
// It uses the same manual-commit discipline as the Review Service: apply first,
// commit second, so a crash redelivers rather than drops. Re-applying a result
// is naturally idempotent (it sets the status to the same terminal value).
type ResultConsumer struct {
	consumer *kafkax.Consumer
	svc      *Service
}

func NewResultConsumer(consumer *kafkax.Consumer, svc *Service) *ResultConsumer {
	return &ResultConsumer{consumer: consumer, svc: svc}
}

// Run consumes until ctx is cancelled.
func (rc *ResultConsumer) Run(ctx context.Context) {
	log := logger.L()
	log.Info("content result-consumer started")
	for {
		msg, err := rc.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("fetch review result", zap.Error(err))
			continue
		}
		var result event.ReviewResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			// A malformed result is not retryable; commit past it so it doesn't
			// wedge the partition.
			log.Error("malformed review result, skipping", zap.Error(err))
			_ = rc.consumer.Commit(ctx, msg)
			continue
		}
		if err := rc.svc.ApplyResult(ctx, result); err != nil {
			// Leave uncommitted so it is redelivered and retried.
			log.Error("apply review result", zap.Uint("post_id", result.PostID), zap.Error(err))
			continue
		}
		if err := rc.consumer.Commit(ctx, msg); err != nil {
			log.Warn("commit review result offset", zap.Error(err))
		}
	}
}
