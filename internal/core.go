package internal

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"sort"
	"strings"

	"github.com/go-pg/pg/v10"
	"github.com/uptrace/bun"
)

// AdapterName represents the ORM adapter type
type AdapterName string

const (
	AdapterNameBun  AdapterName = "bun"
	AdapterNameGoPg AdapterName = "go-pg"
)

// Key represents an encryption key with its ID
type Key struct {
	ID     string
	Value  []byte
	cipher cipher.AEAD
}

// Config holds the configuration for govault
type Config struct {
	AdapterName  AdapterName
	Keys         map[string][]byte
	DefaultKeyID string
	DebugMode    bool
	BunDB        *bun.DB
	GoPgDB       *pg.DB
}

// GovaultDB is the main vault database struct
type GovaultDB struct {
	keys       map[string]*Key
	defaultKey string
	DB         any
}

// New creates a new govault DB with the given configuration
func New(config Config) (*GovaultDB, error) {
	if len(config.Keys) == 0 {
		return nil, fmt.Errorf("at least one encryption key is required")
	}

	if config.DefaultKeyID == "" {
		return nil, fmt.Errorf("default key ID is required")
	}

	if _, exists := config.Keys[config.DefaultKeyID]; !exists {
		return nil, fmt.Errorf("default key ID '%s' not found in keys", config.DefaultKeyID)
	}

	// Initialize keys
	keys := make(map[string]*Key)
	for keyID, keyBytes := range config.Keys {
		key, err := newKey(keyID, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize key '%s': %w", keyID, err)
		}
		keys[keyID] = key
	}

	govault := &GovaultDB{
		keys:       keys,
		defaultKey: config.DefaultKeyID,
	}

	return govault, nil
}

// newKey creates a new encryption key
func newKey(keyID string, keyBytes []byte) (*Key, error) {
	if len(keyBytes) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes for AES-256, got %d bytes", len(keyBytes))
	}

	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &Key{
		ID:     keyID,
		Value:  keyBytes,
		cipher: aead,
	}, nil
}

// GetKeyIDs returns all available key IDs
func (g *GovaultDB) GetKeyIDs() []string {
	ids := make([]string, 0, len(g.keys))
	for id := range g.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// GetDefaultKeyID returns the default key ID
func (g *GovaultDB) GetDefaultKeyID() string {
	return g.defaultKey
}

// GetKeyIDFromEncryptedData extracts key_id from encrypted data
func (g *GovaultDB) GetKeyIDFromEncryptedData(encryptedData string) (string, error) {
	if encryptedData == "" {
		return "", nil
	}

	parts := strings.SplitN(encryptedData, "|", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid encrypted data format")
	}

	return parts[0], nil
}
