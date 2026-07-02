// Package content implements the Content Service: the HTTP front door where
// users create posts and query moderation status. On create it writes a pending
// post and produces a Kafka moderation task; it also consumes review results to
// flip a post to approved/rejected.
package content

import (
	"context"
	"errors"

	"github.com/WindyRivers/moderate-pipe/internal/model"
	"gorm.io/gorm"
)

var ErrNotFound = errors.New("post not found")

type Repo struct{ db *gorm.DB }

func NewRepo(db *gorm.DB) *Repo { return &Repo{db: db} }

// CreatePending inserts a post in the pending state and returns it with its
// assigned ID. The post is invisible to readers until moderation approves it.
func (r *Repo) CreatePending(ctx context.Context, p *model.Post) error {
	p.ReviewStatus = model.StatusPending
	return r.db.WithContext(ctx).Create(p).Error
}

// Get returns a post by id regardless of status (used for status lookups).
func (r *Repo) Get(ctx context.Context, id uint) (*model.Post, error) {
	var p model.Post
	err := r.db.WithContext(ctx).First(&p, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &p, err
}

// SetStatus updates a post's moderation status. Called by the result consumer.
func (r *Repo) SetStatus(ctx context.Context, postID uint, status model.ReviewStatus) error {
	return r.db.WithContext(ctx).Model(&model.Post{}).
		Where("id = ?", postID).
		Update("review_status", status).Error
}

// ListApproved returns the visible feed: only approved posts, newest first.
// This is the reader-facing view that proves moderation gates visibility.
func (r *Repo) ListApproved(ctx context.Context, limit int) ([]model.Post, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	var posts []model.Post
	err := r.db.WithContext(ctx).
		Where("review_status = ?", model.StatusApproved).
		Order("id DESC").Limit(limit).Find(&posts).Error
	return posts, err
}
