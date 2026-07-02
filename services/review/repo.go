// Package review implements the Review Service: it consumes moderation tasks,
// runs them through the rule engine (Aho-Corasick sensitive-word scan + basic
// checks + reputation routing), writes results idempotently, and fans decisions
// back to the Content Service. Reliability (manual commits, retries, DLQ,
// degradation, circuit breaking) lives here.
package review

import (
	"context"
	"errors"

	"github.com/WindyRivers/moderate-pipe/internal/model"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type Repo struct{ db *gorm.DB }

func NewRepo(db *gorm.DB) *Repo { return &Repo{db: db} }

// mysqlDuplicateKey is error 1062 (ER_DUP_ENTRY): a unique-index collision.
const mysqlDuplicateKey = 1062

// SaveResultIfAbsent inserts the moderation result and reports whether this call
// actually created it. The unique index on post_id makes the INSERT the
// idempotency point: if two deliveries of the same message race, exactly one
// INSERT wins and the other gets a duplicate-key error, which we translate to
// "already processed" (created=false). This is the concrete implementation of
// "at-least-once delivery + idempotent consumer = effectively exactly-once".
func (r *Repo) SaveResultIfAbsent(ctx context.Context, res *model.ModerationResult) (created bool, err error) {
	err = r.db.WithContext(ctx).Create(res).Error
	if err == nil {
		return true, nil
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == mysqlDuplicateKey {
		return false, nil
	}
	return false, err
}

// Exists reports whether a moderation result already exists for the post. Used
// as a cheap pre-check before doing work, so redelivered messages usually skip
// straight to commit without touching the User Service or the rule engine.
func (r *Repo) Exists(ctx context.Context, postID uint) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.ModerationResult{}).
		Where("post_id = ?", postID).Count(&count).Error
	return count > 0, err
}

// LoadSensitiveWords returns the current block list.
func (r *Repo) LoadSensitiveWords(ctx context.Context) ([]string, error) {
	var rows []model.SensitiveWord
	if err := r.db.WithContext(ctx).Find(&rows).Error; err != nil {
		return nil, err
	}
	words := make([]string, 0, len(rows))
	for _, w := range rows {
		words = append(words, w.Word)
	}
	return words, nil
}

// SeedSensitiveWords inserts a small demo block list if the table is empty, so
// the rejection path works out of the box. The list is intentionally tame.
func (r *Repo) SeedSensitiveWords(ctx context.Context) error {
	var count int64
	if err := r.db.WithContext(ctx).Model(&model.SensitiveWord{}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	seed := []string{"spam", "scam", "casino", "viagra", "违禁品", "暴力"}
	rows := make([]model.SensitiveWord, 0, len(seed))
	for _, w := range seed {
		rows = append(rows, model.SensitiveWord{Word: w})
	}
	return r.db.WithContext(ctx).Create(&rows).Error
}
