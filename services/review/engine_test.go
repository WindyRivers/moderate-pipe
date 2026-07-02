package review

import (
	"strings"
	"testing"

	"github.com/WindyRivers/moderate-pipe/internal/event"
	"github.com/WindyRivers/moderate-pipe/internal/model"
)

func newTestEngine() *Engine {
	return NewEngine([]string{"spam", "casino", "违禁品"})
}

func task(content string, images int) event.ReviewTask {
	return event.ReviewTask{PostID: 1, UserID: 1, Title: "hi", ContentSnapshot: content, ImageCount: images}
}

func TestEngineApproves(t *testing.T) {
	e := newTestEngine()
	d := e.Evaluate(task("a perfectly normal post", 1), 80, false)
	if d.Status != model.StatusApproved {
		t.Fatalf("want approved, got %s (%s)", d.Status, d.Reason)
	}
}

func TestEngineRejectsSensitiveWord(t *testing.T) {
	e := newTestEngine()
	d := e.Evaluate(task("buy cheap casino tokens", 1), 80, false)
	if d.Status != model.StatusRejected || d.MatchedWord != "casino" {
		t.Fatalf("want rejected on casino, got %s word=%q", d.Status, d.MatchedWord)
	}
}

func TestEngineRejectsCJKSensitiveWord(t *testing.T) {
	e := newTestEngine()
	d := e.Evaluate(task("出售违禁品", 0), 80, false)
	if d.Status != model.StatusRejected {
		t.Fatalf("want rejected on CJK word, got %s", d.Status)
	}
}

func TestEngineRejectsEmpty(t *testing.T) {
	e := newTestEngine()
	if d := e.Evaluate(task("", 0), 80, false); d.Status != model.StatusRejected {
		t.Fatalf("want rejected on empty, got %s", d.Status)
	}
}

func TestEngineRejectsTooLong(t *testing.T) {
	e := newTestEngine()
	long := strings.Repeat("x", maxContentRunes+1)
	if d := e.Evaluate(task(long, 0), 80, false); d.Status != model.StatusRejected {
		t.Fatalf("want rejected on length, got %s", d.Status)
	}
}

func TestEngineRejectsTooManyImages(t *testing.T) {
	e := newTestEngine()
	if d := e.Evaluate(task("pics", maxImages+1), 80, false); d.Status != model.StatusRejected {
		t.Fatalf("want rejected on image count, got %s", d.Status)
	}
}

func TestEngineManualReviewLowReputation(t *testing.T) {
	e := newTestEngine()
	d := e.Evaluate(task("clean content", 0), reputationFloor-1, false)
	if d.Status != model.StatusManual {
		t.Fatalf("want manual_review for low reputation, got %s", d.Status)
	}
}

func TestEngineSkipsReputationWhenDegraded(t *testing.T) {
	e := newTestEngine()
	// Even with a low reputation value, degraded=true must skip the gate and
	// approve (fail-open), so a User Service outage doesn't mass-route to manual.
	d := e.Evaluate(task("clean content", 0), 0, true)
	if d.Status != model.StatusApproved {
		t.Fatalf("want approved under degradation, got %s", d.Status)
	}
}
