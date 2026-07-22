package auth

import (
	"strings"
	"testing"
)

func TestGenerateAndParseRoundTrip(t *testing.T) {
	gen, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	if !strings.HasPrefix(gen.Token, tokenPrefix+"_") {
		t.Errorf("token %q missing prefix %q", gen.Token, tokenPrefix)
	}
	if len(gen.SecretHash) != 32 {
		t.Errorf("SecretHash length = %d, want 32 (sha256)", len(gen.SecretHash))
	}

	keyID, secret, err := ParseToken(gen.Token)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if keyID != gen.KeyID {
		t.Errorf("parsed key id = %v, want %v", keyID, gen.KeyID)
	}
	if !Verify(secret, gen.SecretHash) {
		t.Error("Verify() = false for the matching secret, want true")
	}
}

func TestGenerateKeyIsUnique(t *testing.T) {
	a, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	b, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if a.KeyID == b.KeyID {
		t.Error("two generated keys share a key id")
	}
	if a.Token == b.Token {
		t.Error("two generated keys share a token")
	}
}

func TestParseTokenRejectsMalformed(t *testing.T) {
	valid, _ := GenerateKey()
	keyID, _, _ := ParseToken(valid.Token)

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"no prefix", keyID.String() + "_" + strings.Repeat("a", 64)},
		{"wrong prefix", "bearer_" + keyID.String() + "_" + strings.Repeat("a", 64)},
		{"missing secret", "notif_" + keyID.String()},
		{"empty secret", "notif_" + keyID.String() + "_"},
		{"bad key id", "notif_not-a-uuid_" + strings.Repeat("a", 64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := ParseToken(tt.token); err == nil {
				t.Errorf("ParseToken(%q) = nil error, want error", tt.token)
			}
		})
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	gen, _ := GenerateKey()
	if Verify("clearly-the-wrong-secret", gen.SecretHash) {
		t.Error("Verify() = true for a wrong secret, want false")
	}
}
