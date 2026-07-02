// Package event defines the message contracts exchanged over Kafka between the
// services. Keeping them in one shared package guarantees the producer
// (Content Service) and consumer (Review Service) can never drift on the wire
// format.
package event

import "time"

// ReviewTask is published to post-review-topic when a post is created. It
// carries a *snapshot* of the content, not just the post_id.
//
// Why embed the content snapshot instead of only the ID? Two reasons:
//  1. Consistency: if the Review Service re-queried MySQL by ID, it might read
//     a version of the post that was edited (or, on read replicas, not yet
//     replicated) after the event was produced — it would moderate different
//     bytes than the ones that triggered the event. The snapshot pins exactly
//     what is being judged.
//  2. Decoupling & load: the Review Service can moderate without a synchronous
//     read back into the Content Service's database, removing a coupling point
//     and a burst of read load that would otherwise track the posting rate.
//
// The trade-off is message size: large posts inflate the topic. For a text-post
// platform the snapshot is a few KB, well under Kafka's default max message
// size, so the consistency win is worth it. For very large payloads (video,
// long documents) the usual pattern is a "claim check" — put the blob in object
// storage and pass a URL — which we note in the README but don't need here.
type ReviewTask struct {
	PostID          uint      `json:"post_id"`
	UserID          uint      `json:"user_id"`
	Title           string    `json:"title"`
	ContentSnapshot string    `json:"content_snapshot"`
	ImageCount      int       `json:"image_count"`
	SubmittedAt     time.Time `json:"submitted_at"`
}

// ReviewResult is published to review-result-topic after moderation completes,
// notifying the Content Service to refresh its cached status.
type ReviewResult struct {
	PostID     uint      `json:"post_id"`
	UserID     uint      `json:"user_id"`
	Status     string    `json:"status"` // approved | rejected | manual_review
	Reason     string    `json:"reason"`
	Degraded   bool      `json:"degraded"`
	ReviewedAt time.Time `json:"reviewed_at"`
}
