// Package user implements the User Service: a gRPC server that owns the users
// table and serves reputation/profile lookups to the Review Service.
package user

import (
	"context"
	"errors"

	"github.com/WindyRivers/moderate-pipe/internal/model"
	"gorm.io/gorm"
)

// ErrNotFound is returned when a user id has no row.
var ErrNotFound = errors.New("user not found")

// Repo is the persistence layer for users.
type Repo struct{ db *gorm.DB }

func NewRepo(db *gorm.DB) *Repo { return &Repo{db: db} }

// Get returns a user by id.
func (r *Repo) Get(ctx context.Context, id uint) (*model.User, error) {
	var u model.User
	err := r.db.WithContext(ctx).First(&u, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	return &u, err
}

// ApplyViolation records one more violation for a user and recomputes the
// reputation score. The scoring rule is intentionally simple and monotonic:
// reputation = 80 - 10*violations, floored at 0. It is called by the Review
// Service when a post is rejected. In a stricter service-ownership design this
// would be a dedicated RPC or a Kafka event consumed by the User Service; here
// the services share one database (Project 1's schema), so a guarded UPDATE is
// the pragmatic choice, documented as such in the README.
func (r *Repo) ApplyViolation(ctx context.Context, userID uint) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var u model.User
		if err := tx.First(&u, userID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNotFound
			}
			return err
		}
		u.ViolationCount++
		u.Reputation = 80 - 10*u.ViolationCount
		if u.Reputation < 0 {
			u.Reputation = 0
		}
		return tx.Model(&u).Updates(map[string]interface{}{
			"violation_count": u.ViolationCount,
			"reputation":      u.Reputation,
		}).Error
	})
}

// Seed inserts a few demo users if the table is empty, so the pipeline can be
// exercised immediately after `docker compose up` without a separate fixture
// step. It includes one deliberately low-reputation user to exercise the
// manual-review routing path.
func (r *Repo) Seed(ctx context.Context) error {
	var count int64
	if err := r.db.WithContext(ctx).Model(&model.User{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	users := []model.User{
		{Username: "alice", Email: "alice@example.com", PasswordHash: "x", Reputation: 80, ViolationCount: 0},
		{Username: "bob", Email: "bob@example.com", PasswordHash: "x", Reputation: 80, ViolationCount: 0},
		{Username: "troll", Email: "troll@example.com", PasswordHash: "x", Reputation: 20, ViolationCount: 6},
	}
	return r.db.WithContext(ctx).Create(&users).Error
}
