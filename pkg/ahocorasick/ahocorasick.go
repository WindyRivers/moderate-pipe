// Package ahocorasick implements the Aho-Corasick multi-pattern string matching
// automaton, used by the Review Service to scan post content against a
// sensitive-word block list.
//
// Why Aho-Corasick instead of the obvious "for each word, strings.Contains"?
// Brute force is O(n * m * k): for text of length n and k patterns of average
// length m, every pattern rescans the whole text. Aho-Corasick builds a single
// automaton once (a trie plus "failure links" that behave like KMP's failure
// function generalised to many patterns) and then scans the text exactly once,
// in O(n + z) where z is the number of matches — independent of how many words
// are on the block list. For a moderation service whose block list grows to
// thousands of words while every post must be scanned on the hot path, that is
// the difference between linear and quadratic work.
//
// The matcher is rune-based so it works for CJK block lists (the sensitive-word
// use case here is bilingual), not just ASCII bytes.
package ahocorasick

// node is a single state in the automaton. children are keyed by rune; fail is
// the failure link (the longest proper suffix of this state's path that is also
// a prefix of some pattern); output collects the patterns that end at this
// state (including those reachable via failure links, merged at build time).
type node struct {
	children map[rune]*node
	fail     *node
	output   []string
	depth    int
}

// Matcher is an immutable, built automaton. It is safe for concurrent use by
// multiple goroutines because Match only reads.
type Matcher struct {
	root     *node
	patterns int
}

// New builds an automaton from the given patterns. Empty patterns are ignored.
// Matching is case-insensitive for ASCII: patterns and input are folded to
// lower case, which is the common expectation for a word block list.
func New(patterns []string) *Matcher {
	root := &node{children: map[rune]*node{}}
	count := 0

	// Phase 1: build the trie of patterns (goto function).
	for _, p := range patterns {
		p = fold(p)
		if p == "" {
			continue
		}
		cur := root
		for _, r := range p {
			next, ok := cur.children[r]
			if !ok {
				next = &node{children: map[rune]*node{}, depth: cur.depth + 1}
				cur.children[r] = next
			}
			cur = next
		}
		cur.output = append(cur.output, p)
		count++
	}

	// Phase 2: build failure links via BFS. The root and depth-1 nodes fail to
	// the root; deeper nodes fail to the node reached by following their
	// parent's failure link on the same rune. Outputs are merged along the
	// failure chain so a single state lookup yields every matching pattern.
	queue := make([]*node, 0)
	for _, child := range root.children {
		child.fail = root
		queue = append(queue, child)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for r, child := range cur.children {
			// Find the failure target for child.
			f := cur.fail
			for f != nil {
				if nxt, ok := f.children[r]; ok {
					child.fail = nxt
					break
				}
				f = f.fail
			}
			if child.fail == nil {
				child.fail = root
			}
			child.output = append(child.output, child.fail.output...)
			queue = append(queue, child)
		}
	}

	return &Matcher{root: root, patterns: count}
}

// Match reports whether the text contains any pattern, and returns the first
// matched pattern found (empty string if none). Returning the first match is
// enough for the moderation decision (reject on any hit) while still naming the
// offending word for the audit log.
func (m *Matcher) Match(text string) (bool, string) {
	cur := m.root
	for _, r := range fold(text) {
		// Follow failure links until we can consume r or hit the root.
		for cur != m.root {
			if _, ok := cur.children[r]; ok {
				break
			}
			cur = cur.fail
		}
		if nxt, ok := cur.children[r]; ok {
			cur = nxt
		}
		if len(cur.output) > 0 {
			return true, cur.output[0]
		}
	}
	return false, ""
}

// MatchAll returns every distinct pattern that occurs in the text. Used by
// tests and by callers that want the full set of violations.
func (m *Matcher) MatchAll(text string) []string {
	seen := map[string]struct{}{}
	var hits []string
	cur := m.root
	for _, r := range fold(text) {
		for cur != m.root {
			if _, ok := cur.children[r]; ok {
				break
			}
			cur = cur.fail
		}
		if nxt, ok := cur.children[r]; ok {
			cur = nxt
		}
		for _, w := range cur.output {
			if _, dup := seen[w]; !dup {
				seen[w] = struct{}{}
				hits = append(hits, w)
			}
		}
	}
	return hits
}

// Size returns the number of patterns loaded (used for logging/metrics).
func (m *Matcher) Size() int { return m.patterns }

// fold lower-cases ASCII letters while leaving non-ASCII runes (e.g. CJK)
// untouched. A full Unicode case fold is unnecessary for a block list and would
// pull in more surface area than the moderation use case warrants.
func fold(s string) string {
	b := []rune(s)
	for i, r := range b {
		if r >= 'A' && r <= 'Z' {
			b[i] = r + ('a' - 'A')
		}
	}
	return string(b)
}
