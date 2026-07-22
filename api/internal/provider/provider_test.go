package provider

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

func TestRegistryFor(t *testing.T) {
	email := NewLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "email")
	reg := Registry{"email": email}

	tests := []struct {
		name    string
		channel string
		wantErr error
	}{
		{name: "registered channel resolves", channel: "email"},
		{name: "unregistered channel is a config fault", channel: "sms", wantErr: ErrNoProvider},
		{name: "empty channel is a config fault", channel: "", wantErr: ErrNoProvider},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := reg.For(tt.channel)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("For(%q) error = %v, want %v", tt.channel, err, tt.wantErr)
			}
			if tt.wantErr == nil && p == nil {
				t.Errorf("For(%q) returned a nil provider without an error", tt.channel)
			}
		})
	}
}

func TestLogDeliverSucceeds(t *testing.T) {
	l := NewLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "email")
	if err := l.Deliver(context.Background(), Message{ID: uuid.New(), Recipient: "a@b.c"}); err != nil {
		t.Errorf("Deliver: %v", err)
	}
}
