package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// EffectiveFingerprint returns a deterministic SHA-256 fingerprint of cfg's
// effective values. Callers should pass a configuration after loading so that
// defaults and config-relative path resolution are reflected in the digest.
// JSON object keys, including string-keyed maps, are encoded in lexical order.
func EffectiveFingerprint(cfg *Config) (string, error) {
	if cfg == nil {
		return "", errors.New("config: cannot fingerprint a nil config")
	}

	encoded, err := json.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("config: encode effective config for fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
