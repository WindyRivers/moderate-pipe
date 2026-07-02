package review

import (
	"unicode/utf8"

	"github.com/WindyRivers/moderate-pipe/internal/event"
	"github.com/WindyRivers/moderate-pipe/internal/model"
	"github.com/WindyRivers/moderate-pipe/pkg/ahocorasick"
)

// Rule engine thresholds. Kept as constants for clarity; a real system would
// load these from config so policy can change without a redeploy.
const (
	maxContentRunes = 5000 // reject absurdly long posts
	maxImages       = 9    // typical UGC image-grid cap
	reputationFloor = 30   // below this, route to manual review
)

// Decision is the rule engine's verdict on one post.
type Decision struct {
	Status      model.ReviewStatus
	Reason      string
	MatchedWord string
}

// Engine holds the compiled Aho-Corasick automaton and applies the rule set. It
// is immutable after construction and safe for concurrent use, so all worker
// goroutines share one instance.
type Engine struct {
	matcher *ahocorasick.Matcher
}

// NewEngine builds the engine from the sensitive-word block list.
func NewEngine(sensitiveWords []string) *Engine {
	return &Engine{matcher: ahocorasick.New(sensitiveWords)}
}

// Evaluate applies the rules in priority order and returns the first decisive
// outcome. Order matters: a hard content violation (sensitive word) rejects
// outright regardless of reputation, while a low-reputation author only downs a
// post to manual review. `degraded` signals that the reputation value is a
// fallback default because the User Service was unreachable — in that case we
// skip the reputation gate entirely rather than act on an unreliable number.
func (e *Engine) Evaluate(task event.ReviewTask, reputation int, degraded bool) Decision {
	// Rule 1: length bounds.
	n := utf8.RuneCountInString(task.ContentSnapshot)
	if n == 0 && task.ImageCount == 0 {
		return Decision{Status: model.StatusRejected, Reason: "empty post"}
	}
	if n > maxContentRunes {
		return Decision{Status: model.StatusRejected, Reason: "content exceeds length limit"}
	}

	// Rule 2: image count.
	if task.ImageCount > maxImages {
		return Decision{Status: model.StatusRejected, Reason: "too many images"}
	}

	// Rule 3: sensitive-word scan (title + body) via Aho-Corasick.
	if ok, word := e.matcher.Match(task.Title + " " + task.ContentSnapshot); ok {
		return Decision{Status: model.StatusRejected, Reason: "sensitive word detected", MatchedWord: word}
	}

	// Rule 4: reputation routing. Skipped under degradation so a User Service
	// outage cannot mass-route everyone to manual review.
	if !degraded && reputation < reputationFloor {
		return Decision{Status: model.StatusManual, Reason: "low author reputation"}
	}

	return Decision{Status: model.StatusApproved, Reason: "passed all rules"}
}
