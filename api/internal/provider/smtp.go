package provider

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// SMTPConfig describes the mail server to deliver through.
type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	// From is the envelope sender and the From header.
	From string
	// StartTLS upgrades the connection after greeting. It should be on for any
	// real server and off for a local test server that does not offer TLS.
	StartTLS bool
	// InsecureSkipVerify disables certificate verification. Local development
	// only — it defeats the point of TLS.
	InsecureSkipVerify bool
}

// SMTP delivers notifications as email.
type SMTP struct {
	cfg    SMTPConfig
	dialer *net.Dialer
}

// NewSMTP constructs an SMTP provider.
func NewSMTP(cfg SMTPConfig) *SMTP {
	return &SMTP{cfg: cfg, dialer: &net.Dialer{}}
}

// emailPayload is the payload shape this provider understands.
type emailPayload struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
	HTML    bool   `json:"html"`
}

// Deliver sends one message as email.
//
// Every error is classified: a malformed payload or an unusable address is
// permanent, because the identical request will fail identically forever.
// Network and server-side failures stay transient so backoff can retry them.
func (s *SMTP) Deliver(ctx context.Context, msg Message) error {
	payload, err := parseEmailPayload(msg.Payload)
	if err != nil {
		// The client sent something this provider cannot turn into an email.
		// No amount of retrying changes that.
		return Permanent(err)
	}

	if _, err := mail.ParseAddress(msg.Recipient); err != nil {
		return Permanent(fmt.Errorf("invalid recipient %q: %w", msg.Recipient, err))
	}

	body := buildMessage(s.cfg.From, msg.Recipient, payload)

	return s.send(ctx, msg.Recipient, body)
}

// send performs the SMTP conversation, honoring ctx throughout.
//
// net/smtp has no context-aware API, so the deadline is applied in two places:
// DialContext for establishing the connection, and a socket deadline for every
// exchange after it. Without the second, a server that accepts the connection
// and then stops responding would hang this worker indefinitely — the failure
// mode a timeout is supposed to prevent.
func (s *SMTP) send(ctx context.Context, recipient string, body []byte) error {
	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))

	conn, err := s.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("connecting to smtp server %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("setting connection deadline: %w", err)
		}
	}

	client, err := smtp.NewClient(conn, s.cfg.Host)
	if err != nil {
		return fmt.Errorf("starting smtp session: %w", err)
	}
	defer func() { _ = client.Close() }()

	if s.cfg.StartTLS {
		if err := client.StartTLS(&tls.Config{
			ServerName:         s.cfg.Host,
			InsecureSkipVerify: s.cfg.InsecureSkipVerify, //nolint:gosec // opt-in, documented as development only
			MinVersion:         tls.VersionTLS12,
		}); err != nil {
			return fmt.Errorf("starting tls: %w", err)
		}
	}

	if s.cfg.Username != "" {
		auth := smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
		if err := client.Auth(auth); err != nil {
			// Bad credentials will not fix themselves, but this also covers
			// "auth temporarily unavailable", so it stays transient and is
			// surfaced loudly by the attempt history instead.
			return fmt.Errorf("smtp authentication: %w", err)
		}
	}

	if err := client.Mail(s.cfg.From); err != nil {
		return fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	if err := client.Rcpt(recipient); err != nil {
		// A rejected recipient is usually permanent (no such mailbox), and a
		// 5xx says so explicitly.
		if isPermanentSMTPError(err) {
			return Permanent(fmt.Errorf("smtp RCPT TO %s: %w", recipient, err))
		}
		return fmt.Errorf("smtp RCPT TO %s: %w", recipient, err)
	}

	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("writing message body: %w", err)
	}
	if err := w.Close(); err != nil {
		if isPermanentSMTPError(err) {
			return Permanent(fmt.Errorf("completing message: %w", err))
		}
		return fmt.Errorf("completing message: %w", err)
	}

	// Quit failing after the message was accepted does not un-send it, so the
	// error is not worth failing the delivery over.
	_ = client.Quit()
	return nil
}

// parseEmailPayload extracts the email fields from a notification payload.
func parseEmailPayload(raw json.RawMessage) (emailPayload, error) {
	var p emailPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return emailPayload{}, fmt.Errorf("payload is not a valid email payload: %w", err)
	}
	if strings.TrimSpace(p.Subject) == "" {
		return emailPayload{}, errors.New("payload.subject is required for the email channel")
	}
	if strings.TrimSpace(p.Body) == "" {
		return emailPayload{}, errors.New("payload.body is required for the email channel")
	}
	return p, nil
}

// buildMessage renders RFC 5322 message bytes.
//
// Header values are encoded rather than interpolated raw: a subject containing
// a newline would otherwise inject arbitrary headers, which is how open relays
// and spoofed senders happen.
func buildMessage(from, to string, p emailPayload) []byte {
	contentType := "text/plain; charset=UTF-8"
	if p.HTML {
		contentType = "text/html; charset=UTF-8"
	}

	var b strings.Builder
	b.WriteString("From: " + sanitizeHeader(from) + "\r\n")
	b.WriteString("To: " + sanitizeHeader(to) + "\r\n")
	// Q-encoding keeps non-ASCII subjects intact and neutralizes newlines.
	b.WriteString("Subject: " + mime.QEncoding.Encode("UTF-8", sanitizeHeader(p.Subject)) + "\r\n")
	b.WriteString("Date: " + time.Now().Format(time.RFC1123Z) + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: " + contentType + "\r\n")
	b.WriteString("\r\n")
	b.WriteString(normalizeBody(p.Body))

	return []byte(b.String())
}

// sanitizeHeader strips CR and LF so a value cannot inject extra headers.
func sanitizeHeader(v string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(v)
}

// normalizeBody converts line endings to the CRLF the protocol requires.
//
// It deliberately does NOT escape leading dots. SMTP treats a lone "." on its
// own line as end-of-message, so dots must be stuffed — but net/smtp's Data()
// returns a textproto.DotWriter that already does exactly that. Doing it here
// as well double-escapes, and the recipient sees ".." where the sender wrote
// ".". Dot stuffing belongs to the transport, which is the only layer that
// knows whether it has already been applied.
func normalizeBody(body string) string {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	return strings.ReplaceAll(body, "\n", "\r\n")
}

// isPermanentSMTPError reports whether an SMTP reply is a 5xx, which the
// protocol defines as a permanent failure — as opposed to a 4xx, which
// explicitly means "try again later" (greylisting, mailbox full, rate limits).
// Honoring that distinction is the difference between backing off politely and
// hammering a server that has already told you no.
func isPermanentSMTPError(err error) bool {
	var tp *textproto.Error
	if errors.As(err, &tp) {
		return tp.Code >= 500 && tp.Code < 600
	}
	return false
}
