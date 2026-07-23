package http

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// NotificationReader is the persistence capability the read handlers need.
// Every method is tenant-scoped by construction: there is no way to reach a
// notification without supplying the client id it must belong to.
type NotificationReader interface {
	List(ctx context.Context, f store.ListFilter) ([]store.Notification, error)
	GetForClient(ctx context.Context, clientID, id uuid.UUID) (store.Notification, error)
	ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]store.Attempt, error)
}

// notificationDetail is the read representation of a notification. It is
// separate from the 202 ingestion response on purpose: that response promises
// only "accepted", while this one reports observed delivery state.
type notificationDetail struct {
	ID             string    `json:"id"`
	Channel        string    `json:"channel"`
	Recipient      string    `json:"recipient"`
	Payload        any       `json:"payload"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	IdempotencyKey string    `json:"idempotency_key"`
	ScheduledAt    time.Time `json:"scheduled_at"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// attemptResponse is one entry of a notification's delivery history.
type attemptResponse struct {
	ID            string    `json:"id"`
	AttemptNumber int       `json:"attempt_number"`
	Outcome       string    `json:"outcome"`
	Error         string    `json:"error,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	DurationMS    int64     `json:"duration_ms"`
}

// listResponse is a page of notifications plus the cursor for the next one.
// next_cursor is null on the last page, which is how a client knows to stop.
type listResponse struct {
	Data       []notificationDetail `json:"data"`
	NextCursor *string              `json:"next_cursor"`
}

// handleListNotifications returns a page of the authenticated client's
// notifications, newest first.
func handleListNotifications(reader NotificationReader) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID, ok := ClientIDFrom(req.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		q := req.URL.Query()

		status := store.Status(q.Get("status"))
		if status != "" && !validStatus(status) {
			writeError(w, http.StatusBadRequest, "unknown status", "status")
			return
		}

		channel := store.Channel(q.Get("channel"))
		if channel != "" && !validChannel(channel) {
			writeError(w, http.StatusBadRequest, "unknown channel", "channel")
			return
		}

		limit, err := parseLimit(q.Get("limit"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "limit")
			return
		}

		cursor, err := decodeCursor(q.Get("cursor"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor", "cursor")
			return
		}

		// Ask for one more row than requested. Its presence is what proves
		// another page exists, without a second COUNT query over the table.
		notifications, err := reader.List(req.Context(), store.ListFilter{
			ClientID: clientID,
			Status:   status,
			Channel:  channel,
			Cursor:   cursor,
			Limit:    limit + 1,
		})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}

		var next *string
		if len(notifications) > limit {
			notifications = notifications[:limit]
			encoded := encodeCursor(notifications[len(notifications)-1].ID)
			next = &encoded
		}

		data := make([]notificationDetail, 0, len(notifications))
		for _, n := range notifications {
			data = append(data, toDetail(n))
		}
		writeJSON(w, http.StatusOK, listResponse{Data: data, NextCursor: next})
	}
}

// handleGetNotification returns one of the client's notifications.
func handleGetNotification(reader NotificationReader) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID, ok := ClientIDFrom(req.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		id, err := uuid.Parse(chi.URLParam(req, "id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid notification id", "id")
			return
		}

		n, err := reader.GetForClient(req.Context(), clientID, id)
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "notification not found"})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}

		writeJSON(w, http.StatusOK, toDetail(n))
	}
}

// handleListAttempts returns a notification's delivery history.
//
// Ownership is checked first, by loading the notification for this client. That
// lookup is what stops one tenant reading another's failure messages by id, and
// it makes an unknown id and someone else's id indistinguishable.
func handleListAttempts(reader NotificationReader) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID, ok := ClientIDFrom(req.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		id, err := uuid.Parse(chi.URLParam(req, "id"))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid notification id", "id")
			return
		}

		if _, err := reader.GetForClient(req.Context(), clientID, id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "notification not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}

		attempts, err := reader.ListAttempts(req.Context(), id)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}

		data := make([]attemptResponse, 0, len(attempts))
		for _, a := range attempts {
			data = append(data, attemptResponse{
				ID:            a.ID.String(),
				AttemptNumber: a.AttemptNumber,
				Outcome:       string(a.Outcome),
				Error:         a.Error,
				StartedAt:     a.StartedAt,
				FinishedAt:    a.FinishedAt,
				DurationMS:    a.Duration().Milliseconds(),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": data})
	}
}

// toDetail converts a stored notification to its read representation. Payload
// is emitted as raw JSON rather than a re-encoded string so clients receive the
// object they sent.
func toDetail(n store.Notification) notificationDetail {
	return notificationDetail{
		ID:             n.ID.String(),
		Channel:        string(n.Channel),
		Recipient:      n.Recipient,
		Payload:        rawJSON(n.Payload),
		Status:         string(n.Status),
		Attempts:       n.Attempts,
		MaxAttempts:    n.MaxAttempts,
		IdempotencyKey: n.IdempotencyKey,
		ScheduledAt:    n.ScheduledAt,
		CreatedAt:      n.CreatedAt,
		UpdatedAt:      n.UpdatedAt,
	}
}

// rawJSON wraps stored jsonb so it is emitted verbatim.
type rawJSON []byte

// MarshalJSON returns the stored bytes unchanged.
func (r rawJSON) MarshalJSON() ([]byte, error) {
	if len(r) == 0 {
		return []byte("null"), nil
	}
	return r, nil
}

// parseLimit validates the page-size parameter, defaulting when absent.
func parseLimit(raw string) (int, error) {
	if raw == "" {
		return store.DefaultListLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	if n < 1 {
		return 0, errors.New("limit must be at least 1")
	}
	if n > store.MaxListLimit {
		return 0, errors.New("limit exceeds the maximum of " + strconv.Itoa(store.MaxListLimit))
	}
	return n, nil
}

// encodeCursor renders a page position as an opaque token.
//
// It is base64 rather than a bare id to discourage clients from constructing or
// reasoning about cursors. The format is then free to change — to a
// (timestamp, id) tuple, say — without breaking callers who only ever echo it
// back.
func encodeCursor(id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id.String()))
}

// decodeCursor parses a cursor token, returning uuid.Nil for an absent one.
func decodeCursor(raw string) (uuid.UUID, error) {
	if raw == "" {
		return uuid.Nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(string(decoded))
}

// validStatus reports whether a status names a real lifecycle state, so a typo
// returns 400 rather than an empty page that looks like "no results".
func validStatus(s store.Status) bool {
	switch s {
	case store.StatusPending, store.StatusQueued, store.StatusDelivering,
		store.StatusDelivered, store.StatusFailed, store.StatusDeadLettered:
		return true
	default:
		return false
	}
}

// validChannel reports whether a channel is one the system delivers on.
func validChannel(c store.Channel) bool {
	switch c {
	case store.ChannelEmail, store.ChannelSMS, store.ChannelPush:
		return true
	default:
		return false
	}
}
