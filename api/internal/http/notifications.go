package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Adityashukla2006/notification-system/api/internal/notification"
)

// maxBodyBytes caps the accepted request body. Rejecting oversized bodies before
// reading them is a cheap guard against a client (or attacker) streaming an
// unbounded payload into memory.
const maxBodyBytes = 64 * 1024

// NotificationCreator is the service capability the handler needs. Depending on
// an interface keeps the handler testable with a fake.
type NotificationCreator interface {
	Create(ctx context.Context, in notification.CreateInput) (notification.Result, error)
}

// createNotificationRequest is the accepted JSON body.
type createNotificationRequest struct {
	Channel     string          `json:"channel"`
	Recipient   string          `json:"recipient"`
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt *time.Time      `json:"scheduled_at"`
	MaxAttempts *int            `json:"max_attempts"`
}

// notificationResponse is the 202 body.
type notificationResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// handleCreateNotification accepts a notification: validate, hand to the
// service, and return 202. The client id comes from the authenticated context,
// never the body — a caller cannot claim to be another tenant.
func handleCreateNotification(svc NotificationCreator) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if ct := req.Header.Get("Content-Type"); ct != "" && !isJSON(ct) {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "content-type must be application/json"})
			return
		}

		idempotencyKey := req.Header.Get("Idempotency-Key")
		if idempotencyKey == "" {
			writeError(w, http.StatusBadRequest, "missing Idempotency-Key header", "idempotency_key")
			return
		}

		clientID, ok := ClientIDFrom(req.Context())
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}

		req.Body = http.MaxBytesReader(w, req.Body, maxBodyBytes)
		dec := json.NewDecoder(req.Body)
		var body createNotificationRequest
		if err := dec.Decode(&body); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
				return
			}
			writeError(w, http.StatusBadRequest, "invalid JSON body", "")
			return
		}

		result, err := svc.Create(req.Context(), notification.CreateInput{
			ClientID:       clientID,
			IdempotencyKey: idempotencyKey,
			Channel:        body.Channel,
			Recipient:      body.Recipient,
			Payload:        body.Payload,
			ScheduledAt:    body.ScheduledAt,
			MaxAttempts:    body.MaxAttempts,
		})
		if err != nil {
			if ve, ok := notification.AsValidationError(err); ok {
				writeError(w, http.StatusBadRequest, ve.Message, ve.Field)
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}

		if result.Outcome == notification.OutcomeConflict {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "idempotency key was reused with a different request",
			})
			return
		}

		// Both a new acceptance and an idempotent replay return 202: delivery is
		// at-least-once and asynchronous, so the response only ever means
		// "accepted", never "delivered".
		n := result.Notification
		writeJSON(w, http.StatusAccepted, notificationResponse{
			ID:        n.ID.String(),
			Status:    string(n.Status),
			CreatedAt: n.CreatedAt,
		})
	}
}

// isJSON reports whether a Content-Type header names JSON, ignoring parameters
// like "; charset=utf-8".
func isJSON(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.TrimSpace(mediaType) == "application/json"
}

// writeError writes a JSON error with an optional field for validation failures.
func writeError(w http.ResponseWriter, status int, message, field string) {
	body := map[string]string{"error": message}
	if field != "" {
		body["field"] = field
	}
	writeJSON(w, status, body)
}
