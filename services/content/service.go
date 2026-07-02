package content

import (
	"context"
	"errors"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/event"
	"github.com/WindyRivers/moderate-pipe/internal/model"
	"github.com/WindyRivers/moderate-pipe/pkg/kafkax"
	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"github.com/WindyRivers/moderate-pipe/pkg/ratelimit"
	"go.uber.org/zap"
)

// ErrRateLimited is returned when the create endpoint is over its limit.
var ErrRateLimited = errors.New("rate limited")

// Service is the Content Service business logic.
type Service struct {
	repo     *Repo
	cache    *StatusCache
	producer *kafkax.Producer
	limiter  *ratelimit.Limiter
}

func NewService(repo *Repo, cache *StatusCache, producer *kafkax.Producer, limiter *ratelimit.Limiter) *Service {
	return &Service{repo: repo, cache: cache, producer: producer, limiter: limiter}
}

// CreatePostInput is the create request.
type CreatePostInput struct {
	UserID  uint
	Title   string
	Content string
	Images  []string
}

// CreatePost persists a pending post and enqueues a moderation task.
//
// The DB write and the Kafka publish are two separate systems, so there is a
// dual-write hazard: if the process dies between them, a post could sit pending
// with no task enqueued. We accept that small window here and document the
// production fix (the transactional outbox pattern) in the README; the write
// order — DB first, then publish — is chosen so the failure mode is a stuck
// pending post that a sweeper can re-enqueue, never a moderation task pointing
// at a post that does not exist.
func (s *Service) CreatePost(ctx context.Context, in CreatePostInput) (*model.Post, error) {
	// Rate limit the endpoint globally to shield the downstream pipeline from
	// bursts. Kafka smooths sustained load; this guards against a thundering
	// herd outrunning even the queue.
	if !s.limiter.Allow(ctx, "post:create") {
		return nil, ErrRateLimited
	}

	post := &model.Post{
		UserID:  in.UserID,
		Title:   in.Title,
		Content: in.Content,
		Images:  in.Images,
	}
	if err := s.repo.CreatePending(ctx, post); err != nil {
		return nil, err
	}

	// Seed the cache with the known-pending status so an immediate status poll
	// is a cache hit.
	s.cache.Set(ctx, post.ID, model.StatusPending)

	task := event.ReviewTask{
		PostID:          post.ID,
		UserID:          post.UserID,
		Title:           post.Title,
		ContentSnapshot: post.Content,
		ImageCount:      len(post.Images),
		SubmittedAt:     time.Now(),
	}
	if err := s.producer.Publish(ctx, itoa(post.ID), task); err != nil {
		// The post exists but wasn't enqueued. Surface the error; the caller
		// sees a 500 and can retry. A background sweeper (not built here) would
		// re-enqueue posts stuck pending past a threshold.
		logger.L().Error("publish review task failed", zap.Uint("post_id", post.ID), zap.Error(err))
		return nil, err
	}
	logger.L().Info("post created and enqueued",
		zap.Uint("post_id", post.ID), zap.Uint("user_id", post.UserID))
	return post, nil
}

// GetStatus returns a post's current moderation status, cache-first.
func (s *Service) GetStatus(ctx context.Context, postID uint) (model.ReviewStatus, error) {
	if st, ok := s.cache.Get(ctx, postID); ok {
		return st, nil
	}
	p, err := s.repo.Get(ctx, postID)
	if err != nil {
		return "", err
	}
	s.cache.Set(ctx, p.ID, p.ReviewStatus)
	return p.ReviewStatus, nil
}

// ApplyResult is called by the review-result consumer: it flips the stored
// status and refreshes the cache so readers and status polls see the decision.
func (s *Service) ApplyResult(ctx context.Context, r event.ReviewResult) error {
	status := model.ReviewStatus(r.Status)
	if err := s.repo.SetStatus(ctx, r.PostID, status); err != nil {
		return err
	}
	s.cache.Set(ctx, r.PostID, status)
	logger.L().Info("applied review result",
		zap.Uint("post_id", r.PostID), zap.String("status", r.Status),
		zap.Bool("degraded", r.Degraded))
	return nil
}

// Feed returns the visible (approved-only) feed.
func (s *Service) Feed(ctx context.Context, limit int) ([]model.Post, error) {
	return s.repo.ListApproved(ctx, limit)
}
