package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/notification"
	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// fakeCreator is a NotificationCreator whose result and error are fixed, and
// which records the input it received.
type fakeCreator struct {
	result   notification.Result
	err      error
	gotInput notification.CreateInput
	called   bool
}

func (f *fakeCreator) Create(_ context.Context, in notification.CreateInput) (notification.Result, error) {
	f.called = true
	f.gotInput = in
	return f.result, f.err
}

// acceptedResult builds a Result as the service would return for a new accept.
func acceptedResult() notification.Result {
	return notification.Result{
		Notification: store.Notification{
			ID:        uuid.New(),
			Status:    store.StatusQueued,
			CreatedAt: time.Now(),
		},
		Outcome: notification.OutcomeCreated,
	}
}

func postNotification(t *testing.T, creator NotificationCreator, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	handler := handleCreateNotification(creator)

	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "idem-123")
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	// The handler reads the client id from context; auth middleware would set it.
	ctx := context.WithValue(req.Context(), clientIDKey, uuid.New())
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestHandleCreateNotification(t *testing.T) {
	validBody := `{"channel":"email","recipient":"a@b.com","payload":{"subject":"hi"}}`

	tests := []struct {
		name       string
		body       string
		headers    map[string]string
		creator    *fakeCreator
		wantStatus int
	}{
		{
			name:       "accepted returns 202",
			body:       validBody,
			creator:    &fakeCreator{result: acceptedResult()},
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "replay returns 202",
			body:       validBody,
			creator:    &fakeCreator{result: notification.Result{Notification: store.Notification{ID: uuid.New(), Status: store.StatusQueued}, Outcome: notification.OutcomeReplayed}},
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "conflict returns 409",
			body:       validBody,
			creator:    &fakeCreator{result: notification.Result{Outcome: notification.OutcomeConflict}},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "validation error returns 400",
			body:       validBody,
			creator:    &fakeCreator{err: notification.ValidationError{Field: "channel", Message: "bad"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "service error returns 500",
			body:       validBody,
			creator:    &fakeCreator{err: context.DeadlineExceeded},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "missing idempotency key returns 400",
			body:       validBody,
			headers:    map[string]string{"Idempotency-Key": ""},
			creator:    &fakeCreator{result: acceptedResult()},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wrong content type returns 415",
			body:       validBody,
			headers:    map[string]string{"Content-Type": "text/plain"},
			creator:    &fakeCreator{result: acceptedResult()},
			wantStatus: http.StatusUnsupportedMediaType,
		},
		{
			name:       "malformed json returns 400",
			body:       `{"channel":`,
			creator:    &fakeCreator{result: acceptedResult()},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := postNotification(t, tt.creator, tt.body, tt.headers)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestCreateNotificationUsesContextClientID proves the client id comes from the
// authenticated context, not the request body.
func TestCreateNotificationUsesContextClientID(t *testing.T) {
	creator := &fakeCreator{result: acceptedResult()}
	handler := handleCreateNotification(creator)

	wantClientID := uuid.New()
	body := `{"channel":"email","recipient":"a@b.com","payload":{"x":1},"client_id":"00000000-0000-0000-0000-000000000000"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "k1")
	req = req.WithContext(context.WithValue(req.Context(), clientIDKey, wantClientID))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if !creator.called {
		t.Fatal("service was not called")
	}
	if creator.gotInput.ClientID != wantClientID {
		t.Errorf("service got client id %v, want %v (from context)", creator.gotInput.ClientID, wantClientID)
	}
	if creator.gotInput.IdempotencyKey != "k1" {
		t.Errorf("service got idempotency key %q, want %q", creator.gotInput.IdempotencyKey, "k1")
	}
}

// Test202BodyShape checks the accepted response body carries id and status.
func Test202BodyShape(t *testing.T) {
	res := acceptedResult()
	rec := postNotification(t, &fakeCreator{result: res}, `{"channel":"email","recipient":"a@b.com","payload":{"x":1}}`, nil)

	var got notificationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if got.ID != res.Notification.ID.String() {
		t.Errorf("id = %q, want %q", got.ID, res.Notification.ID.String())
	}
	if got.Status != string(store.StatusQueued) {
		t.Errorf("status = %q, want %q", got.Status, store.StatusQueued)
	}
}
