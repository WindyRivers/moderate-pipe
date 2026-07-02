package content

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/WindyRivers/moderate-pipe/pkg/logger"
	"go.uber.org/zap"
)

// Handler exposes the Content Service over HTTP using only the standard
// library — the moderation pipeline is the interesting part of this project, so
// the front door is kept dependency-free rather than pulling in a web framework
// (Project 1 already demonstrates Gin).
type Handler struct {
	svc   *Service
	ready func() bool
}

func NewHandler(svc *Service, ready func() bool) *Handler {
	return &Handler{svc: svc, ready: ready}
}

// Routes wires the HTTP mux.
func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/readyz", h.readyz)
	mux.HandleFunc("/posts", h.posts)          // POST create, GET feed
	mux.HandleFunc("/posts/", h.postStatus)    // GET /posts/{id}/status
	return mux
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if h.ready != nil && !h.ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createRequest struct {
	UserID  uint     `json:"user_id"`
	Title   string   `json:"title"`
	Content string   `json:"content"`
	Images  []string `json:"images"`
}

func (h *Handler) posts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createPost(w, r)
	case http.MethodGet:
		h.feed(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) createPost(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.UserID == 0 || strings.TrimSpace(req.Title) == "" {
		writeError(w, http.StatusBadRequest, "user_id and title are required")
		return
	}
	post, err := h.svc.CreatePost(r.Context(), CreatePostInput{
		UserID:  req.UserID,
		Title:   req.Title,
		Content: req.Content,
		Images:  req.Images,
	})
	if err != nil {
		if errors.Is(err, ErrRateLimited) {
			writeError(w, http.StatusTooManyRequests, "rate limited: too many posts, slow down")
			return
		}
		logger.L().Error("create post", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to create post")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{
		"post_id":       post.ID,
		"review_status": post.ReviewStatus,
		"message":       "post accepted and queued for moderation",
	})
}

func (h *Handler) feed(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	posts, err := h.svc.Feed(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load feed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"posts": posts})
}

// postStatus handles GET /posts/{id}/status.
func (h *Handler) postStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// Path form: /posts/{id}/status
	rest := strings.TrimPrefix(r.URL.Path, "/posts/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[1] != "status" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid post id")
		return
	}
	status, err := h.svc.GetStatus(r.Context(), uint(id))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "post not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get status")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"post_id":       id,
		"review_status": status,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
