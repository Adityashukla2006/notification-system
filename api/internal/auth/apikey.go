// Package auth contains the primitives for API-key authentication: generating a
// key, parsing a presented token, and verifying a secret against a stored hash.
// These are pure functions with no database or HTTP dependencies, so the
// cryptographic logic can be tested in isolation.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// tokenPrefix labels every token so a leaked key is recognizable (e.g. in logs
// or secret scanners) as belonging to this service.
const tokenPrefix = "notif"

// secretBytes is the length of the random secret half. 32 bytes = 256 bits of
// CSPRNG output, far beyond brute-force reach, which is why a fast hash is
// sufficient to store it.
const secretBytes = 32

// ErrMalformedToken is returned when a presented token does not have the
// expected prefix/keyid/secret shape.
var ErrMalformedToken = errors.New("malformed api key token")

// GeneratedKey is the result of minting a new API key. The Token is the only
// time the raw secret exists in plaintext — it must be shown to the caller once
// and then is unrecoverable, because only SecretHash is persisted.
type GeneratedKey struct {
	// KeyID is the public identifier stored as api_keys.id and embedded in the
	// token; it is the lookup handle at authentication time.
	KeyID uuid.UUID
	// Token is the full string handed to the caller: notif_<keyid>_<secret>.
	Token string
	// SecretHash is the SHA-256 of the secret half. This, not the secret, is
	// what gets stored.
	SecretHash []byte
}

// GenerateKey mints a new API key with a random KeyID and secret. It returns
// the full token to hand to the caller once and the hash to persist.
func GenerateKey() (GeneratedKey, error) {
	keyID, err := uuid.NewRandom()
	if err != nil {
		return GeneratedKey{}, fmt.Errorf("generating key id: %w", err)
	}

	secret := make([]byte, secretBytes)
	if _, err := rand.Read(secret); err != nil {
		return GeneratedKey{}, fmt.Errorf("generating secret: %w", err)
	}
	// Hex, not base64url: base64url's alphabet includes '_', which is our token
	// separator, so hex keeps parsing unambiguous.
	secretHex := hex.EncodeToString(secret)

	hash := HashSecret(secretHex)

	return GeneratedKey{
		KeyID:      keyID,
		Token:      fmt.Sprintf("%s_%s_%s", tokenPrefix, keyID.String(), secretHex),
		SecretHash: hash,
	}, nil
}

// HashSecret returns the SHA-256 of a secret. It is the single definition of
// how secrets are hashed, used by both generation and verification so they
// cannot drift.
func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// ParseToken splits a presented token into its key id and secret. It validates
// only the shape, not the secret — verification is a separate step against the
// stored hash.
func ParseToken(token string) (keyID uuid.UUID, secret string, err error) {
	parts := strings.SplitN(token, "_", 3)
	if len(parts) != 3 || parts[0] != tokenPrefix {
		return uuid.Nil, "", ErrMalformedToken
	}

	keyID, err = uuid.Parse(parts[1])
	if err != nil {
		return uuid.Nil, "", ErrMalformedToken
	}
	if parts[2] == "" {
		return uuid.Nil, "", ErrMalformedToken
	}
	return keyID, parts[2], nil
}

// Verify reports whether a presented secret matches a stored hash. The
// comparison is constant-time so that response timing cannot leak how much of
// the hash matched.
func Verify(secret string, storedHash []byte) bool {
	return subtle.ConstantTimeCompare(HashSecret(secret), storedHash) == 1
}
