package ahocorasick

import (
	"strings"
	"testing"
)

func TestMatchBasic(t *testing.T) {
	m := New([]string{"he", "she", "his", "hers"})
	cases := []struct {
		text string
		want bool
	}{
		{"ushers", true},   // "she" and "hers" both occur
		{"his story", true},
		{"nothing here", true}, // "he" occurs in "here"... actually in "here"
		{"quiet", false},
	}
	for _, c := range cases {
		got, _ := m.Match(c.text)
		if got != c.want {
			t.Errorf("Match(%q) = %v, want %v", c.text, got, c.want)
		}
	}
}

func TestMatchOverlapping(t *testing.T) {
	// Classic Aho-Corasick overlap case: patterns share suffixes/prefixes.
	m := New([]string{"abc", "bcd", "cde"})
	all := m.MatchAll("abcde")
	want := map[string]bool{"abc": true, "bcd": true, "cde": true}
	if len(all) != 3 {
		t.Fatalf("MatchAll = %v, want 3 matches", all)
	}
	for _, w := range all {
		if !want[w] {
			t.Errorf("unexpected match %q", w)
		}
	}
}

func TestCaseInsensitive(t *testing.T) {
	m := New([]string{"badword"})
	if ok, _ := m.Match("This is a BadWord!"); !ok {
		t.Error("expected case-insensitive match")
	}
}

func TestCJK(t *testing.T) {
	m := New([]string{"暴力", "违禁品"})
	if ok, w := m.Match("这里有暴力内容"); !ok || w != "暴力" {
		t.Errorf("expected CJK match 暴力, got ok=%v w=%q", ok, w)
	}
	if ok, _ := m.Match("正常内容"); ok {
		t.Error("did not expect a match in clean CJK text")
	}
}

func TestNoFalsePositiveAcrossFailureLinks(t *testing.T) {
	m := New([]string{"announce", "nce"})
	// "nce" is a suffix of "announce"; make sure the failure-link output merge
	// reports it even when "announce" isn't fully present.
	if ok, w := m.Match("since then"); !ok || w != "nce" {
		t.Errorf("expected suffix match nce, got ok=%v w=%q", ok, w)
	}
}

func TestEmptyAndClean(t *testing.T) {
	m := New([]string{"", "spam"})
	if m.Size() != 1 {
		t.Errorf("empty pattern should be ignored, size=%d", m.Size())
	}
	if ok, _ := m.Match(strings.Repeat("clean ", 100)); ok {
		t.Error("clean text should not match")
	}
}
