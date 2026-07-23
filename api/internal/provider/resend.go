package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"strings"
)

// resendEndpoint is Resend's send-email API.
const resendEndpoint = "https://api.resend.com/emails"

// maxResendErrorBytes caps how much of an error response is read, so a
// misbehaving endpoint cannot stream an unbounded body into an error message.
const maxResendErrorBytes = 4 * 1024

// ResendConfig configures the Resend provider.
type ResendConfig struct {
	// APIKey authenticates with Resend. It is read from the environment and
	// never hardcoded: a key in source is a key in every clone and every log.
	APIKey string
	// From is the sender address. It must belong to a domain verified with
	// Resend, or be their onboarding@resend.dev sandbox sender.
	From string
	// BaseURL overrides the endpoint, for tests.
	BaseURL string
	// HTTPClient overrides the client, for tests. Timeouts come from the
	// request context rather than the client, so the worker's per-delivery
	// deadline governs.
	HTTPClient *http.Client
}

// Resend delivers notifications as email through Resend's HTTP API.
//
// It speaks HTTP directly rather than using the vendor SDK: the API is a single
// POST, and going direct keeps the dependency list short, makes the context
// deadline authoritative, and leaves error classification — which decides
// whether a failure is retried or dead-lettered — under this package's control.
type Resend struct {
	cfg    ResendConfig
	client *http.Client
	url    string
}

// NewResend constructs a Resend provider.
func NewResend(cfg ResendConfig) *Resend {
	client := cfg.HTTPClient
	if client == nil {
		// No client timeout: the per-delivery context deadline is the single
		// source of truth for how long a send may take. Two competing timeouts
		// would only make the effective limit harder to reason about.
		client = &http.Client{}
	}
	url := cfg.BaseURL
	if url == "" {
		url = resendEndpoint
	}
	return &Resend{cfg: cfg, client: client, url: url}
}

// resendRequest is the JSON body Resend expects.
type resendRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	HTML    string   `json:"html,omitempty"`
	Text    string   `json:"text,omitempty"`
}

// resendResponse is the success body; only the id is of interest.
type resendResponse struct {
	ID string `json:"id"`
}

// resendError is Resend's error body.
type resendError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

// Deliver sends one message as email via Resend.
func (r *Resend) Deliver(ctx context.Context, msg Message) error {
	if r.cfg.APIKey == "" {
		// Misconfiguration, not a transient fault: no retry will supply a key.
		return Permanent(fmt.Errorf("resend api key is not configured"))
	}

	payload, err := parseEmailPayload(msg.Payload)
	if err != nil {
		return Permanent(err)
	}
	if _, err := mail.ParseAddress(msg.Recipient); err != nil {
		return Permanent(fmt.Errorf("invalid recipient %q: %w", msg.Recipient, err))
	}

	body := resendRequest{
		From:    r.cfg.From,
		To:      []string{msg.Recipient},
		Subject: payload.Subject,
	}
	if payload.HTML {
		body.HTML = payload.Body
	} else {
		body.Text = payload.Body
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return Permanent(fmt.Errorf("encoding resend request: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("building resend request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+r.cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")

	// Delivery is at-least-once, so this notification may genuinely be sent
	// twice — after a worker dies mid-send, or when a response is lost and the
	// attempt is retried. Keying on the notification id lets Resend collapse
	// the duplicate instead of the recipient receiving the same mail twice.
	// Harmless if the header is ignored.
	req.Header.Set("Idempotency-Key", msg.ID.String())

	resp, err := r.client.Do(req)
	if err != nil {
		// Transport failures (dial, TLS, timeout) are transient by nature.
		return fmt.Errorf("calling resend: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out resendResponse
		// A success we cannot parse is still a success: the mail was accepted,
		// and failing here would resend it.
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxResendErrorBytes)).Decode(&out)
		return nil
	}

	detail := readResendError(resp.Body)
	err = fmt.Errorf("resend returned %d: %s", resp.StatusCode, detail)

	if isPermanentHTTPStatus(resp.StatusCode) {
		return Permanent(err)
	}
	return err
}

// isPermanentHTTPStatus classifies a response code.
//
// Most 4xx responses mean the request itself is wrong — a malformed body, an
// unverified sender, a rejected address — and will fail identically forever, so
// retrying wastes attempts. The exceptions are the two that explicitly invite a
// retry: 408 (timeout) and 429 (rate limited). Everything 5xx is the server's
// problem and may well pass on the next try.
func isPermanentHTTPStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return code >= 400 && code < 500
}

// readResendError extracts a human-readable reason from an error response,
// falling back to the raw body when it is not the expected shape.
func readResendError(body io.Reader) string {
	raw, err := io.ReadAll(io.LimitReader(body, maxResendErrorBytes))
	if err != nil || len(raw) == 0 {
		return "no error detail returned"
	}

	var e resendError
	if err := json.Unmarshal(raw, &e); err == nil && e.Message != "" {
		if e.Name != "" {
			return e.Name + ": " + e.Message
		}
		return e.Message
	}
	return strings.TrimSpace(string(raw))
}
