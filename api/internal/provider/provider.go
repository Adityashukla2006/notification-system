// Package provider is the pluggable delivery layer: one implementation per
// channel, selected at runtime by the worker.
//
// The interface is deliberately narrow — one method, taking a value type with
// no database or queue types in it. That keeps a provider a pure function of
// "here is a message, send it", so adding a channel means writing one Deliver
// method and registering it, with no changes to the worker loop.
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// ErrNoProvider is returned when no provider is registered for a channel. It is
// a configuration fault, not a transient one: retrying will not fix it.
var ErrNoProvider = errors.New("no provider registered for channel")

// Message is everything a provider needs to perform one delivery. It carries no
// delivery-state fields (attempts, status) because a provider must not make
// decisions about retries — that is the worker's job alone.
type Message struct {
	// ID is the notification id, carried for correlation in provider logs.
	ID uuid.UUID
	// Recipient is the channel-specific destination: an address, phone
	// number, or device token.
	Recipient string
	// Payload is the client-supplied JSON body.
	Payload json.RawMessage
}

// Provider delivers a message over exactly one channel.
//
// A nil error means the provider believes the message was accepted downstream.
// Any non-nil error is a failed attempt; the worker decides what happens next.
type Provider interface {
	Deliver(ctx context.Context, msg Message) error
}

// Registry maps a channel name to the provider that serves it. The channel is a
// plain string rather than the store's Channel type so that providers stay
// independent of the persistence layer.
type Registry map[string]Provider

// For returns the provider registered for channel, or ErrNoProvider.
func (r Registry) For(channel string) (Provider, error) {
	p, ok := r[channel]
	if !ok {
		return nil, fmt.Errorf("%q: %w", channel, ErrNoProvider)
	}
	return p, nil
}

// Log is a Provider that records what it would have sent instead of sending it.
// It exists so the delivery pipeline can be exercised end to end before any real
// integration is wired up, and it doubles as a safe default in development.
type Log struct {
	logger  *slog.Logger
	channel string
}

// NewLog constructs a Log provider that labels its output with channel.
func NewLog(logger *slog.Logger, channel string) Log {
	return Log{logger: logger, channel: channel}
}

// Deliver records the message and always reports success.
func (l Log) Deliver(_ context.Context, msg Message) error {
	l.logger.Info("delivered notification (log provider)",
		"notification_id", msg.ID,
		"channel", l.channel,
		"recipient", msg.Recipient,
		"payload_bytes", len(msg.Payload),
	)
	return nil
}
