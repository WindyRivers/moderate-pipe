// Package model holds the GORM data models shared by the services. It reuses
// Project 1 (ContentHub)'s User/Post shape and extends it with the moderation
// pipeline's state: a review status on the post, a per-post ModerationResult
// row (which doubles as the idempotency guard), and a reputation field on the
// user.
package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

// StringSlice is stored as a JSON column in MySQL (reused verbatim from
// ContentHub) so a post can carry a list of image URLs without a join table.
type StringSlice []string

func (s StringSlice) Value() (driver.Value, error) {
	if s == nil {
		return "[]", nil
	}
	return json.Marshal(s)
}

func (s *StringSlice) Scan(value interface{}) error {
	if value == nil {
		*s = StringSlice{}
		return nil
	}
	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return errors.New("unsupported type for StringSlice")
	}
	if len(bytes) == 0 {
		*s = StringSlice{}
		return nil
	}
	return json.Unmarshal(bytes, s)
}

// ReviewStatus is the moderation state of a post. Unlike ContentHub's simple
// active/deleted flag, a post here starts life invisible (pending) and only
// becomes visible after the Review Service approves it — this is the whole
// point of the asynchronous pipeline.
type ReviewStatus string

const (
	StatusPending  ReviewStatus = "pending"       // enqueued, not yet moderated
	StatusApproved ReviewStatus = "approved"      // passed the rule engine, visible
	StatusRejected ReviewStatus = "rejected"      // failed a rule (e.g. sensitive word)
	StatusManual   ReviewStatus = "manual_review" // routed to a human queue
)

// User mirrors ContentHub's account, plus a Reputation score and a running
// ViolationCount. The User Service owns this table and serves Reputation over
// gRPC to the Review Service.
type User struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	Username       string    `gorm:"type:varchar(64);uniqueIndex;not null" json:"username"`
	Email          string    `gorm:"type:varchar(128);uniqueIndex;not null" json:"email"`
	PasswordHash   string    `gorm:"type:varchar(255);not null" json:"-"`
	Reputation     int       `gorm:"not null;default:80" json:"reputation"`
	ViolationCount int       `gorm:"not null;default:0" json:"violation_count"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Post is a piece of content. ReviewStatus governs visibility: only approved
// posts are returned to other users. ImageCount is denormalised from Images so
// the rule engine can check it without deserialising JSON.
type Post struct {
	ID           uint         `gorm:"primaryKey" json:"id"`
	UserID       uint         `gorm:"index;not null" json:"user_id"`
	Title        string       `gorm:"type:varchar(255);not null" json:"title"`
	Content      string       `gorm:"type:text" json:"content"`
	Images       StringSlice  `gorm:"type:json" json:"images"`
	ReviewStatus ReviewStatus `gorm:"type:varchar(16);not null;default:'pending';index" json:"review_status"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

// ModerationResult is the authoritative outcome of moderating a post. Its
// unique index on PostID is the idempotency guard: the Review Service does an
// INSERT-or-skip keyed on post_id, so redelivering the same Kafka message can
// never produce two results or double-decrement a user's reputation. This is
// the "at-least-once delivery + idempotent consumer = effectively
// exactly-once" pattern in one table.
type ModerationResult struct {
	ID         uint         `gorm:"primaryKey" json:"id"`
	PostID     uint         `gorm:"uniqueIndex;not null" json:"post_id"`
	UserID     uint         `gorm:"index;not null" json:"user_id"`
	Status     ReviewStatus `gorm:"type:varchar(16);not null" json:"status"`
	Reason     string       `gorm:"type:varchar(255)" json:"reason"`
	MatchedWord string      `gorm:"type:varchar(64)" json:"matched_word,omitempty"`
	// Degraded records that the reputation check was skipped because the User
	// Service was unavailable — surfaced so degraded decisions are auditable.
	Degraded  bool      `gorm:"not null;default:false" json:"degraded"`
	CreatedAt time.Time `json:"created_at"`
}

// SensitiveWord is a row in the maintained block list. Keeping it in MySQL lets
// the list be updated without a redeploy; the Review Service loads it into an
// Aho-Corasick automaton at startup.
type SensitiveWord struct {
	ID   uint   `gorm:"primaryKey" json:"id"`
	Word string `gorm:"type:varchar(64);uniqueIndex;not null" json:"word"`
}

// AllModels lists every model for AutoMigrate.
func AllModels() []interface{} {
	return []interface{}{
		&User{},
		&Post{},
		&ModerationResult{},
		&SensitiveWord{},
	}
}
