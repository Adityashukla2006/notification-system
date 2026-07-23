package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// resendServer stands in for Resend, capturing the request and replying with a
// scripted status and body.
type resendServer struct {
	*httptest.Server
	lastBody    resendRequest
	lastAuth    string
	lastIdemKey string
	calls       int
}

func newResendServer(t *testing.T, status int, body string) *resendServer {
	t.Helper()
	rs := &resendServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.calls++
		rs.lastAuth = r.Header.Get("Authorization")
		rs.lastIdemKey = r.Header.Get("Idempotency-Key")

		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &rs.lastBody)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(rs.Close)
	return rs
}

func testResend(srv *resendServer) *Resend {
	return NewResend(ResendConfig{
		APIKey:  "re_test_key",
		From:    "noreply@example.com",
		BaseURL: srv.URL,
	})
}

func testMessage() Message {
	return Message{
		ID:        uuid.New(),
		Recipient: "user@example.com",
		Payload:   json.RawMessage(`{"subject":"Hello","body":"Hi there"}`),
	}
}

func TestResendDeliverSuccess(t *testing.T) {
	srv := newResendServer(t, http.StatusOK, `{"id":"abc-123"}`)
	p := testResend(srv)
	msg := testMessage()

	if err := p.Deliver(context.Background(), msg); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	if srv.lastBody.From != "noreply@example.com" {
		t.Errorf("from = %q, want the configured sender", srv.lastBody.From)
	}
	if len(srv.lastBody.To) != 1 || srv.lastBody.To[0] != "user@example.com" {
		t.Errorf("to = %v, want [user@example.com]", srv.lastBody.To)
	}
	if srv.lastBody.Subject != "Hello" {
		t.Errorf("subject = %q, want Hello", srv.lastBody.Subject)
	}
	if srv.lastBody.Text != "Hi there" {
		t.Errorf("text = %q, want the body", srv.lastBody.Text)
	}
	if srv.lastBody.HTML != "" {
		t.Errorf("html = %q, want empty for a non-html payload", srv.lastBody.HTML)
	}
	if srv.lastAuth != "Bearer re_test_key" {
		t.Errorf("authorization = %q, want a bearer token", srv.lastAuth)
	}
}

func TestResendSendsHTMLWhenRequested(t *testing.T) {
	srv := newResendServer(t, http.StatusOK, `{"id":"abc"}`)
	p := testResend(srv)

	err := p.Deliver(context.Background(), Message{
		ID:        uuid.New(),
		Recipient: "user@example.com",
		Payload:   json.RawMessage(`{"subject":"Hi","body":"<p>Hi</p>","html":true}`),
	})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if srv.lastBody.HTML != "<p>Hi</p>" {
		t.Errorf("html = %q, want the body", srv.lastBody.HTML)
	}
	if srv.lastBody.Text != "" {
		t.Errorf("text = %q, want empty for an html payload", srv.lastBody.Text)
	}
}

// TestResendSendsIdempotencyKey matters because delivery is at-least-once: the
// same notification can genuinely be sent twice, and the key is what stops the
// recipient seeing it twice.
func TestResendSendsIdempotencyKey(t *testing.T) {
	srv := newResendServer(t, http.StatusOK, `{"id":"abc"}`)
	p := testResend(srv)
	msg := testMessage()

	if err := p.Deliver(context.Background(), msg); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if srv.lastIdemKey != msg.ID.String() {
		t.Errorf("Idempotency-Key = %q, want the notification id %q", srv.lastIdemKey, msg.ID)
	}

	// A retry of the same notification must reuse the key, or it is useless.
	if err := p.Deliver(context.Background(), msg); err != nil {
		t.Fatalf("Deliver (retry): %v", err)
	}
	if srv.lastIdemKey != msg.ID.String() {
		t.Errorf("retry Idempotency-Key = %q, want the same id %q", srv.lastIdemKey, msg.ID)
	}
}

func TestResendClassifiesResponses(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantErr       bool
		wantPermanent bool
	}{
		{name: "200 succeeds", status: http.StatusOK, body: `{"id":"a"}`},
		{name: "201 succeeds", status: http.StatusCreated, body: `{"id":"a"}`},

		// The request itself is wrong; retrying changes nothing.
		{name: "400 is permanent", status: http.StatusBadRequest, body: `{"message":"bad request"}`, wantErr: true, wantPermanent: true},
		{name: "401 is permanent", status: http.StatusUnauthorized, body: `{"message":"invalid api key"}`, wantErr: true, wantPermanent: true},
		{name: "403 is permanent", status: http.StatusForbidden, body: `{"message":"domain not verified"}`, wantErr: true, wantPermanent: true},
		{name: "422 is permanent", status: http.StatusUnprocessableEntity, body: `{"message":"invalid to field"}`, wantErr: true, wantPermanent: true},

		// These explicitly invite a retry.
		{name: "408 is transient", status: http.StatusRequestTimeout, body: `{}`, wantErr: true},
		{name: "429 is transient", status: http.StatusTooManyRequests, body: `{"message":"slow down"}`, wantErr: true},

		// The server's problem, not ours.
		{name: "500 is transient", status: http.StatusInternalServerError, body: `{}`, wantErr: true},
		{name: "502 is transient", status: http.StatusBadGateway, body: `oops`, wantErr: true},
		{name: "503 is transient", status: http.StatusServiceUnavailable, body: `{}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newResendServer(t, tt.status, tt.body)
			err := testResend(srv).Deliver(context.Background(), testMessage())

			if (err != nil) != tt.wantErr {
				t.Fatalf("Deliver error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && IsPermanent(err) != tt.wantPermanent {
				t.Errorf("permanent = %v, want %v (error: %v)", IsPermanent(err), tt.wantPermanent, err)
			}
		})
	}
}

// TestResendErrorIncludesReason keeps the provider's explanation in the attempt
// history, which is the whole point of recording it.
func TestResendErrorIncludesReason(t *testing.T) {
	srv := newResendServer(t, http.StatusUnprocessableEntity,
		`{"name":"validation_error","message":"The from address is not verified"}`)

	err := testResend(srv).Deliver(context.Background(), testMessage())
	if err == nil {
		t.Fatal("Deliver succeeded, want an error")
	}
	for _, want := range []string{"422", "validation_error", "not verified"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err.Error(), want)
		}
	}
}

func TestResendRejectsUnusableInput(t *testing.T) {
	srv := newResendServer(t, http.StatusOK, `{"id":"a"}`)

	tests := []struct {
		name      string
		apiKey    string
		recipient string
		payload   string
	}{
		{name: "no api key", apiKey: "", recipient: "user@example.com", payload: `{"subject":"a","body":"b"}`},
		{name: "invalid recipient", apiKey: "k", recipient: "nope", payload: `{"subject":"a","body":"b"}`},
		{name: "payload missing subject", apiKey: "k", recipient: "user@example.com", payload: `{"body":"b"}`},
		{name: "payload missing body", apiKey: "k", recipient: "user@example.com", payload: `{"subject":"a"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewResend(ResendConfig{APIKey: tt.apiKey, From: "x@example.com", BaseURL: srv.URL})
			err := p.Deliver(context.Background(), Message{
				ID:        uuid.New(),
				Recipient: tt.recipient,
				Payload:   json.RawMessage(tt.payload),
			})
			if err == nil {
				t.Fatal("Deliver succeeded, want an error")
			}
			if !IsPermanent(err) {
				t.Errorf("error %v is transient, want permanent", err)
			}
		})
	}
}

// TestResendHonorsContext confirms the worker's per-delivery deadline governs,
// rather than the provider hanging on a slow API.
func TestResendHonorsContext(t *testing.T) {
	// The handler is bounded by its own timer as well as the request context.
	// Waiting only on r.Context() risks the handler never returning if the
	// server does not observe the client going away, and Close() blocks on
	// outstanding handlers — which hangs the whole package rather than failing.
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()

	p := NewResend(ResendConfig{APIKey: "k", From: "x@example.com", BaseURL: slow.URL})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.Deliver(ctx, testMessage())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Deliver succeeded against a hanging server, want an error")
	}
	// A timeout may well pass on retry, so it must not be permanent.
	if IsPermanent(err) {
		t.Errorf("timeout classified as permanent: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Deliver took %v, want it bounded by the 100ms context", elapsed)
	}
}

// TestResendSuccessWithUnparseableBody: the mail was accepted, so a response we
// cannot parse must not cause a resend.
func TestResendSuccessWithUnparseableBody(t *testing.T) {
	srv := newResendServer(t, http.StatusOK, `not json at all`)
	if err := testResend(srv).Deliver(context.Background(), testMessage()); err != nil {
		t.Errorf("Deliver = %v, want success: the message was accepted", err)
	}
}
