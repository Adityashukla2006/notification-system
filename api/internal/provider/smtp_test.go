package provider

import (
	"context"
	"encoding/json"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParseEmailPayload(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{name: "subject and body", payload: `{"subject":"Hi","body":"Hello there"}`},
		{name: "html flag is accepted", payload: `{"subject":"Hi","body":"<p>Hi</p>","html":true}`},
		{name: "extra fields are ignored", payload: `{"subject":"Hi","body":"x","unknown":1}`},

		{name: "missing subject", payload: `{"body":"Hello"}`, wantErr: true},
		{name: "missing body", payload: `{"subject":"Hi"}`, wantErr: true},
		{name: "blank subject", payload: `{"subject":"   ","body":"Hello"}`, wantErr: true},
		{name: "blank body", payload: `{"subject":"Hi","body":"  "}`, wantErr: true},
		{name: "not an object", payload: `["nope"]`, wantErr: true},
		{name: "malformed json", payload: `{`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseEmailPayload(json.RawMessage(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Errorf("parseEmailPayload(%s) error = %v, wantErr %v", tt.payload, err, tt.wantErr)
			}
		})
	}
}

// TestDeliverClassifiesUnusableInputAsPermanent is the contract the worker
// relies on: a request that can never succeed must not consume retries.
func TestDeliverClassifiesUnusableInputAsPermanent(t *testing.T) {
	// Host is unreachable on purpose; these failures must be detected before
	// any connection is attempted.
	s := NewSMTP(SMTPConfig{Host: "127.0.0.1", Port: 1, From: "noreply@example.com"})

	tests := []struct {
		name      string
		recipient string
		payload   string
	}{
		{name: "payload missing subject", recipient: "user@example.com", payload: `{"body":"hi"}`},
		{name: "payload missing body", recipient: "user@example.com", payload: `{"subject":"hi"}`},
		{name: "payload not an object", recipient: "user@example.com", payload: `"just a string"`},
		{name: "recipient is not an address", recipient: "not-an-email", payload: `{"subject":"hi","body":"x"}`},
		{name: "recipient is empty", recipient: "", payload: `{"subject":"hi","body":"x"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Deliver(context.Background(), Message{
				ID:        uuid.New(),
				Recipient: tt.recipient,
				Payload:   json.RawMessage(tt.payload),
			})
			if err == nil {
				t.Fatal("Deliver succeeded, want an error")
			}
			if !IsPermanent(err) {
				t.Errorf("error %v is transient, want permanent: retrying cannot fix it", err)
			}
		})
	}
}

// TestDeliverTreatsNetworkFailuresAsTransient is the other half: a server that
// is down right now may well be up in ten seconds, so backoff must apply.
func TestDeliverTreatsNetworkFailuresAsTransient(t *testing.T) {
	// Port 1 on localhost refuses connections.
	s := NewSMTP(SMTPConfig{Host: "127.0.0.1", Port: 1, From: "noreply@example.com"})

	err := s.Deliver(context.Background(), Message{
		ID:        uuid.New(),
		Recipient: "user@example.com",
		Payload:   json.RawMessage(`{"subject":"hi","body":"x"}`),
	})
	if err == nil {
		t.Fatal("Deliver succeeded against a closed port, want an error")
	}
	if IsPermanent(err) {
		t.Errorf("error %v is permanent, want transient: the server may recover", err)
	}
}

// TestDeliverHonorsContext confirms the provider gives up when its deadline
// passes, rather than holding the worker.
func TestDeliverHonorsContext(t *testing.T) {
	// 203.0.113.0/24 is TEST-NET-3, reserved and unroutable, so the dial hangs
	// until the context stops it rather than failing fast.
	s := NewSMTP(SMTPConfig{Host: "203.0.113.1", Port: 25, From: "noreply@example.com"})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.Deliver(ctx, Message{
		ID:        uuid.New(),
		Recipient: "user@example.com",
		Payload:   json.RawMessage(`{"subject":"hi","body":"x"}`),
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Deliver succeeded against an unroutable address, want an error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("Deliver took %v, want it bounded by the 200ms context", elapsed)
	}
}

func TestBuildMessage(t *testing.T) {
	msg := string(buildMessage("sender@example.com", "user@example.com", emailPayload{
		Subject: "Your receipt",
		Body:    "Thanks for your order.",
	}))

	for _, want := range []string{
		"From: sender@example.com\r\n",
		"To: user@example.com\r\n",
		"MIME-Version: 1.0\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n",
		"Thanks for your order.",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q:\n%s", want, msg)
		}
	}

	// Headers and body must be separated by a blank line, or the whole thing
	// is parsed as headers.
	if !strings.Contains(msg, "\r\n\r\n") {
		t.Error("message has no header/body separator")
	}
}

func TestBuildMessageHTML(t *testing.T) {
	msg := string(buildMessage("s@example.com", "u@example.com", emailPayload{
		Subject: "Hi", Body: "<p>Hi</p>", HTML: true,
	}))
	if !strings.Contains(msg, "Content-Type: text/html; charset=UTF-8") {
		t.Errorf("html payload did not set an html content type:\n%s", msg)
	}
}

// TestBuildMessageResistsHeaderInjection is a security property, not a
// formatting nicety: a newline in a header value would let a caller append
// arbitrary headers — a Bcc to a third party, or a forged sender.
func TestBuildMessageResistsHeaderInjection(t *testing.T) {
	msg := string(buildMessage("sender@example.com", "user@example.com", emailPayload{
		Subject: "Hello\r\nBcc: attacker@evil.example",
		Body:    "harmless",
	}))

	headers, _, found := strings.Cut(msg, "\r\n\r\n")
	if !found {
		t.Fatal("message has no header/body separator")
	}

	// The injected text may survive harmlessly INSIDE the Subject value. What
	// must not happen is a new header line: a header only exists if it starts
	// a line, so that is what to assert on.
	for _, line := range strings.Split(headers, "\r\n") {
		if strings.HasPrefix(strings.ToLower(line), "bcc:") {
			t.Errorf("subject injected a Bcc header line:\n%s", headers)
		}
	}

	// And the subject must remain a single header line.
	subjectLines := 0
	for _, line := range strings.Split(headers, "\r\n") {
		if strings.HasPrefix(line, "Subject:") {
			subjectLines++
		}
	}
	if subjectLines != 1 {
		t.Errorf("found %d Subject lines, want exactly 1:\n%s", subjectLines, headers)
	}
}

// TestBuildMessageDoesNotStuffDots pins a bug this code had and a real message
// caught: net/smtp's Data() writer already escapes leading dots, so doing it
// here too delivered ".." where the sender wrote ".". Dot stuffing belongs to
// the transport, and the message must reach it unescaped.
func TestBuildMessageDoesNotStuffDots(t *testing.T) {
	msg := string(buildMessage("s@example.com", "u@example.com", emailPayload{
		Subject: "Hi",
		Body:    "first line\n.\nlast line",
	}))

	_, body, _ := strings.Cut(msg, "\r\n\r\n")
	if strings.Contains(body, "\r\n..\r\n") {
		t.Errorf("dot line was escaped here; the transport escapes it again, so the recipient sees \"..\":\n%q", body)
	}
	if !strings.Contains(body, "\r\n.\r\n") {
		t.Errorf("dot line missing from body:\n%q", body)
	}
	if !strings.Contains(body, "last line") {
		t.Errorf("body lost content after the dot line:\n%q", body)
	}
}

// TestBuildMessageNormalizesLineEndings checks bodies arriving with bare LFs
// are converted to the CRLF the protocol requires.
func TestBuildMessageNormalizesLineEndings(t *testing.T) {
	msg := string(buildMessage("s@example.com", "u@example.com", emailPayload{
		Subject: "Hi", Body: "one\ntwo",
	}))
	_, body, _ := strings.Cut(msg, "\r\n\r\n")

	if strings.Contains(strings.ReplaceAll(body, "\r\n", ""), "\n") {
		t.Errorf("body still contains a bare LF: %q", body)
	}
	if !strings.Contains(body, "one\r\ntwo") {
		t.Errorf("body = %q, want CRLF line endings", body)
	}
}

func TestIsPermanentSMTPError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "550 no such mailbox is permanent", err: &textproto.Error{Code: 550, Msg: "no such user"}, want: true},
		{name: "553 bad address is permanent", err: &textproto.Error{Code: 553, Msg: "bad address"}, want: true},
		// 4xx explicitly means "try again later" — greylisting is the common case.
		{name: "451 greylisting is transient", err: &textproto.Error{Code: 451, Msg: "try again"}, want: false},
		{name: "421 service unavailable is transient", err: &textproto.Error{Code: 421, Msg: "shutting down"}, want: false},
		{name: "non-smtp error is not permanent", err: context.DeadlineExceeded, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPermanentSMTPError(tt.err); got != tt.want {
				t.Errorf("isPermanentSMTPError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPermanentWrapping(t *testing.T) {
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) should stay nil")
	}

	base := context.DeadlineExceeded
	wrapped := Permanent(base)

	if !IsPermanent(wrapped) {
		t.Error("wrapped error not reported as permanent")
	}
	if IsPermanent(base) {
		t.Error("unwrapped error reported as permanent")
	}
	if !strings.Contains(wrapped.Error(), base.Error()) {
		t.Errorf("wrapped error %q lost the original message", wrapped.Error())
	}
}
