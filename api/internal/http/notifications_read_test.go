package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Adityashukla2006/notification-system/api/internal/store"
)

// fakeReader is an in-memory NotificationReader. It enforces the same tenant
// scoping as the real store, so a test cannot pass by accident where the real
// implementation would leak.
type fakeReader struct {
	byID     map[uuid.UUID]store.Notification
	attempts map[uuid.UUID][]store.Attempt
	lastList store.ListFilter
	listErr  error
	getErr   error
}

func newFakeReader(rows ...store.Notification) *fakeReader {
	f := &fakeReader{
		byID:     map[uuid.UUID]store.Notification{},
		attempts: map[uuid.UUID][]store.Attempt{},
	}
	for _, n := range rows {
		f.byID[n.ID] = n
	}
	return f
}

func (f *fakeReader) List(_ context.Context, filter store.ListFilter) ([]store.Notification, error) {
	f.lastList = filter
	if f.listErr != nil {
		return nil, f.listErr
	}

	// Newest first, which for UUIDv7 is descending id order.
	var out []store.Notification
	for _, n := range f.byID {
		if n.ClientID != filter.ClientID {
			continue
		}
		if filter.Status != "" && n.Status != filter.Status {
			continue
		}
		if filter.Channel != "" && n.Channel != filter.Channel {
			continue
		}
		if filter.Cursor != uuid.Nil && n.ID.String() >= filter.Cursor.String() {
			continue
		}
		out = append(out, n)
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j].ID.String() > out[i].ID.String() {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (f *fakeReader) GetForClient(_ context.Context, clientID, id uuid.UUID) (store.Notification, error) {
	if f.getErr != nil {
		return store.Notification{}, f.getErr
	}
	n, ok := f.byID[id]
	// The tenant check is the point: another client's row is ErrNotFound, the
	// same as a row that does not exist.
	if !ok || n.ClientID != clientID {
		return store.Notification{}, store.ErrNotFound
	}
	return n, nil
}

func (f *fakeReader) ListAttempts(_ context.Context, notificationID uuid.UUID) ([]store.Attempt, error) {
	return f.attempts[notificationID], nil
}

// readableNotification builds a stored notification owned by clientID.
func readableNotification(clientID uuid.UUID, status store.Status, channel store.Channel) store.Notification {
	id, _ := uuid.NewV7()
	return store.Notification{
		ID:             id,
		ClientID:       clientID,
		IdempotencyKey: "key-" + id.String(),
		Channel:        channel,
		Recipient:      "user@example.com",
		Payload:        json.RawMessage(`{"body":"hi"}`),
		Status:         status,
		MaxAttempts:    5,
		ScheduledAt:    time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

// readRequest issues an authenticated GET against a router built on reader.
func readRequest(t *testing.T, reader *fakeReader, clientID uuid.UUID, target string) *httptest.ResponseRecorder {
	t.Helper()

	keys := &fakeKeys{}
	token := mint(t, keys, clientID, nil)

	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, keys, &fakeCreator{}, reader, allowAllLimiter{})

	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestListNotifications(t *testing.T) {
	clientID := uuid.New()
	other := uuid.New()

	reader := newFakeReader(
		readableNotification(clientID, store.StatusDelivered, store.ChannelEmail),
		readableNotification(clientID, store.StatusFailed, store.ChannelSMS),
		readableNotification(other, store.StatusDelivered, store.ChannelEmail),
	)

	rec := readRequest(t, reader, clientID, "/v1/notifications")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var body listResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if len(body.Data) != 2 {
		t.Errorf("returned %d notifications, want 2 (the other client's must not appear)", len(body.Data))
	}
	if body.NextCursor != nil {
		t.Errorf("next_cursor = %v, want null on the last page", *body.NextCursor)
	}
}

// TestListNotificationsIsScopedToTheAuthenticatedClient is the isolation
// guarantee: the filter handed to the store must carry the authenticated
// client's id, never anything from user input.
func TestListNotificationsIsScopedToTheAuthenticatedClient(t *testing.T) {
	clientID := uuid.New()
	other := uuid.New()
	reader := newFakeReader(readableNotification(other, store.StatusDelivered, store.ChannelEmail))

	// Even when a caller tries to name another tenant, the scope is unchanged.
	rec := readRequest(t, reader, clientID, "/v1/notifications?client_id="+other.String())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if reader.lastList.ClientID != clientID {
		t.Errorf("list scoped to %s, want the authenticated client %s", reader.lastList.ClientID, clientID)
	}

	var body listResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if len(body.Data) != 0 {
		t.Errorf("returned %d notifications, want 0", len(body.Data))
	}
}

func TestListNotificationsFilters(t *testing.T) {
	clientID := uuid.New()
	reader := newFakeReader(
		readableNotification(clientID, store.StatusDelivered, store.ChannelEmail),
		readableNotification(clientID, store.StatusFailed, store.ChannelSMS),
		readableNotification(clientID, store.StatusFailed, store.ChannelEmail),
	)

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "no filter returns all", query: "", want: 3},
		{name: "status filter", query: "?status=failed", want: 2},
		{name: "channel filter", query: "?channel=email", want: 2},
		{name: "both filters combine", query: "?status=failed&channel=email", want: 1},
		{name: "filter matching nothing", query: "?status=dead_lettered", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := readRequest(t, reader, clientID, "/v1/notifications"+tt.query)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
			}
			var body listResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decoding: %v", err)
			}
			if len(body.Data) != tt.want {
				t.Errorf("returned %d, want %d", len(body.Data), tt.want)
			}
		})
	}
}

func TestListNotificationsRejectsBadParameters(t *testing.T) {
	clientID := uuid.New()
	reader := newFakeReader()

	tests := []struct {
		name  string
		query string
	}{
		{name: "unknown status", query: "?status=exploded"},
		{name: "unknown channel", query: "?channel=telegram"},
		{name: "non-numeric limit", query: "?limit=lots"},
		{name: "zero limit", query: "?limit=0"},
		{name: "negative limit", query: "?limit=-5"},
		{name: "limit above maximum", query: "?limit=1000"},
		{name: "malformed cursor", query: "?cursor=!!!notbase64!!!"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := readRequest(t, reader, clientID, "/v1/notifications"+tt.query)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestListNotificationsPaginates walks every page and checks the cursor both
// advances and terminates, with no row seen twice or skipped.
func TestListNotificationsPaginates(t *testing.T) {
	clientID := uuid.New()
	reader := newFakeReader()
	for range 5 {
		n := readableNotification(clientID, store.StatusDelivered, store.ChannelEmail)
		reader.byID[n.ID] = n
		// UUIDv7 embeds a millisecond timestamp; without a gap the ordering
		// between rows created in the same millisecond is not guaranteed.
		time.Sleep(2 * time.Millisecond)
	}

	seen := map[string]bool{}
	target := "/v1/notifications?limit=2"
	pages := 0

	for {
		pages++
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}

		rec := readRequest(t, reader, clientID, target)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		var body listResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decoding: %v", err)
		}

		for _, n := range body.Data {
			if seen[n.ID] {
				t.Errorf("notification %s returned on more than one page", n.ID)
			}
			seen[n.ID] = true
		}

		if body.NextCursor == nil {
			break
		}
		target = fmt.Sprintf("/v1/notifications?limit=2&cursor=%s", *body.NextCursor)
	}

	if len(seen) != 5 {
		t.Errorf("saw %d notifications across %d pages, want all 5", len(seen), pages)
	}
	if pages != 3 {
		t.Errorf("took %d pages at limit=2, want 3", pages)
	}
}

func TestGetNotification(t *testing.T) {
	clientID := uuid.New()
	other := uuid.New()

	mine := readableNotification(clientID, store.StatusDelivered, store.ChannelEmail)
	theirs := readableNotification(other, store.StatusDelivered, store.ChannelEmail)
	reader := newFakeReader(mine, theirs)

	tests := []struct {
		name       string
		id         string
		wantStatus int
	}{
		{name: "own notification is returned", id: mine.ID.String(), wantStatus: http.StatusOK},
		// Another tenant's row must be indistinguishable from a missing one:
		// a 403 would confirm the id exists.
		{name: "another client's notification is 404", id: theirs.ID.String(), wantStatus: http.StatusNotFound},
		{name: "unknown id is 404", id: uuid.New().String(), wantStatus: http.StatusNotFound},
		{name: "malformed id is 400", id: "not-a-uuid", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := readRequest(t, reader, clientID, "/v1/notifications/"+tt.id)
			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d: %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestGetNotificationReturnsPayloadVerbatim guards against the stored jsonb
// being re-encoded as a string.
func TestGetNotificationReturnsPayloadVerbatim(t *testing.T) {
	clientID := uuid.New()
	n := readableNotification(clientID, store.StatusDelivered, store.ChannelEmail)
	n.Payload = json.RawMessage(`{"subject":"hello","count":3}`)
	reader := newFakeReader(n)

	rec := readRequest(t, reader, clientID, "/v1/notifications/"+n.ID.String())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var body struct {
		Payload struct {
			Subject string `json:"subject"`
			Count   int    `json:"count"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if body.Payload.Subject != "hello" || body.Payload.Count != 3 {
		t.Errorf("payload = %+v, want it emitted as a JSON object", body.Payload)
	}
}

func TestListAttempts(t *testing.T) {
	clientID := uuid.New()
	other := uuid.New()

	mine := readableNotification(clientID, store.StatusFailed, store.ChannelEmail)
	theirs := readableNotification(other, store.StatusFailed, store.ChannelEmail)
	reader := newFakeReader(mine, theirs)

	started := time.Now()
	reader.attempts[mine.ID] = []store.Attempt{{
		ID:            uuid.New(),
		AttemptNumber: 1,
		Outcome:       store.AttemptFailed,
		Error:         "smtp refused",
		StartedAt:     started,
		FinishedAt:    started.Add(250 * time.Millisecond),
	}}
	reader.attempts[theirs.ID] = []store.Attempt{{
		ID:            uuid.New(),
		AttemptNumber: 1,
		Outcome:       store.AttemptFailed,
		Error:         "secret failure detail",
		StartedAt:     started,
		FinishedAt:    started,
	}}

	t.Run("own attempts are returned", func(t *testing.T) {
		rec := readRequest(t, reader, clientID, "/v1/notifications/"+mine.ID.String()+"/attempts")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Data []attemptResponse `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if len(body.Data) != 1 {
			t.Fatalf("returned %d attempts, want 1", len(body.Data))
		}
		if body.Data[0].Error != "smtp refused" {
			t.Errorf("error = %q, want %q", body.Data[0].Error, "smtp refused")
		}
		if body.Data[0].DurationMS != 250 {
			t.Errorf("duration_ms = %d, want 250", body.Data[0].DurationMS)
		}
	})

	// Failure messages can carry sensitive detail, so ownership is checked
	// before any history is read.
	t.Run("another client's attempts are 404", func(t *testing.T) {
		rec := readRequest(t, reader, clientID, "/v1/notifications/"+theirs.ID.String()+"/attempts")
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
		if body := rec.Body.String(); len(body) > 0 && contains(body, "secret failure detail") {
			t.Error("response leaked another client's failure detail")
		}
	})

	t.Run("notification with no attempts returns an empty list", func(t *testing.T) {
		empty := readableNotification(clientID, store.StatusPending, store.ChannelEmail)
		reader.byID[empty.ID] = empty

		rec := readRequest(t, reader, clientID, "/v1/notifications/"+empty.ID.String()+"/attempts")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var body struct {
			Data []attemptResponse `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decoding: %v", err)
		}
		if body.Data == nil {
			t.Error("data was null, want an empty array")
		}
	})
}

func TestReadEndpointsRequireAuthentication(t *testing.T) {
	reader := newFakeReader()
	handler := Router(discardLogger(), fakePinger{}, fakePinger{}, &fakeKeys{}, &fakeCreator{}, reader, allowAllLimiter{})

	for _, target := range []string{
		"/v1/notifications",
		"/v1/notifications/" + uuid.New().String(),
		"/v1/notifications/" + uuid.New().String() + "/attempts",
	} {
		t.Run(target, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, target, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rec.Code)
			}
		})
	}
}

func TestListSurfacesStoreErrorsAs500(t *testing.T) {
	reader := newFakeReader()
	reader.listErr = errors.New("connection refused")

	rec := readRequest(t, reader, uuid.New(), "/v1/notifications")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	id, _ := uuid.NewV7()
	encoded := encodeCursor(id)

	if encoded == id.String() {
		t.Error("cursor is the bare id; it should be opaque so the format can change")
	}

	decoded, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if decoded != id {
		t.Errorf("round trip gave %s, want %s", decoded, id)
	}

	empty, err := decodeCursor("")
	if err != nil {
		t.Fatalf("decodeCursor(\"\"): %v", err)
	}
	if empty != uuid.Nil {
		t.Errorf("empty cursor = %s, want uuid.Nil", empty)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
