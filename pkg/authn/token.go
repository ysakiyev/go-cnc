package authn

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Role byte

const (
	RoleClient Role = 1
	RoleAgent  Role = 2
)

type Claims struct {
	SessionID uuid.UUID
	Role      Role
	RelayID   uuid.UUID
	Exp       time.Time
	KeyID     byte
}

func Mint(keys map[byte][]byte, claims Claims) (string, error) {
	key, ok := keys[claims.KeyID]
	if !ok {
		return "", errors.New("authn: unknown key id")
	}

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}

	body := packBody(claims, nonce)
	sig := sign(key, body)

	return base64.RawURLEncoding.EncodeToString(body) + "." +
		base64.RawURLEncoding.EncodeToString(sig), nil
}

func Verify(keys map[byte][]byte, token string) (Claims, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return Claims{}, errors.New("authn: malformed token")
	}

	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, errors.New("authn: malformed token body")
	}

	claims, nonce, err := unpackBody(body)
	if err != nil {
		return Claims{}, err
	}
	_ = nonce

	key, ok := keys[claims.KeyID]
	if !ok {
		return Claims{}, errors.New("authn: unknown key id")
	}

	gotSig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, errors.New("authn: malformed token sig")
	}

	if !hmac.Equal(gotSig, sign(key, body)) {
		return Claims{}, errors.New("authn: invalid signature")
	}

	if time.Now().After(claims.Exp) {
		return Claims{}, errors.New("authn: token expired")
	}

	return claims, nil
}

func packBody(c Claims, nonce [8]byte) []byte {
	b := make([]byte, 50)
	copy(b[0:16], c.SessionID[:])
	b[16] = byte(c.Role)
	copy(b[17:33], c.RelayID[:])
	binary.BigEndian.PutUint64(b[33:41], uint64(c.Exp.Unix()))
	b[41] = c.KeyID
	copy(b[42:50], nonce[:])
	return b
}

func unpackBody(b []byte) (Claims, [8]byte, error) {
	var nonce [8]byte
	if len(b) != 50 {
		return Claims{}, nonce, errors.New("authn: wrong body length")
	}
	var c Claims
	copy(c.SessionID[:], b[0:16])
	c.Role = Role(b[16])
	copy(c.RelayID[:], b[17:33])
	c.Exp = time.Unix(int64(binary.BigEndian.Uint64(b[33:41])), 0)
	c.KeyID = b[41]
	copy(nonce[:], b[42:50])
	return c, nonce, nil
}

func sign(key, body []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return mac.Sum(nil)
}
