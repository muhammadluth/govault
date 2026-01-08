package govault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
)

// Key represents an encryption key with its ID
type Key struct {
	ID     string
	Value  []byte
	cipher cipher.AEAD
}

// Config holds the configuration for govault
type Config struct {
	Keys         map[string][]byte // key_id -> key_bytes
	DefaultKeyID string            // Active key for encryption
}

// DB is the main govault database wrapper
type DB struct {
	underlying interface{}     // Underlying ORM DB (bun.DB or pg.DB)
	keys       map[string]*Key // All available keys
	defaultKey string          // Default key ID for encryption
	adapter    Adapter         // ORM adapter
}

// Adapter interface for different ORM implementations
type Adapter interface {
	GetName() string
	WrapQueries(db *DB) interface{}
}

// New creates a new govault DB with the given configuration
// Example:
//
//	db, err := govault.New(sqldb, pgdialect.New(), govault.Config{
//	    Keys: map[string][]byte{
//	        "1": []byte("key-1-32-bytes..."),
//	        "2": []byte("key-2-32-bytes..."),
//	    },
//	    DefaultKeyID: "2",
//	})
func New(adapterName string, sqldb *sql.DB, config Config) (*DB, error) {
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

	// Detect adapter based on dialect type
	adapter, err := detectAdapter(adapterName, sqldb)
	if err != nil {
		return nil, err
	}

	db := &DB{
		keys:       keys,
		defaultKey: config.DefaultKeyID,
		adapter:    adapter,
	}

	// Wrap queries with adapter
	db.underlying = adapter.WrapQueries(db)

	return db, nil
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

// detectAdapter detects which ORM adapter to use
func detectAdapter(adapterName string, sqldb *sql.DB) (Adapter, error) {
	// Check for Bun
	if strings.EqualFold(adapterName, "bun") {
		return newBunAdapter(sqldb)
	}

	// Check for go-pg (dialect will be *pg.DB in this case)
	// if strings.Contains(dialectType, "pg.DB") {
	// 	return newGoPgAdapter(dialect)
	// }

	return nil, fmt.Errorf("unsupported ORM: %s", adapterName)
}

// Encrypt encrypts plaintext with the specified key (or default if not specified)
func (db *DB) Encrypt(plaintext string, keyID ...string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	// Determine which key to use
	targetKeyID := db.defaultKey
	if len(keyID) > 0 && keyID[0] != "" {
		targetKeyID = keyID[0]
	}

	key, exists := db.keys[targetKeyID]
	if !exists {
		return "", fmt.Errorf("encryption key '%s' not found", targetKeyID)
	}

	// Generate nonce
	nonce := make([]byte, key.cipher.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt
	ciphertext := key.cipher.Seal(nil, nonce, []byte(plaintext), nil)

	// Encode to base64
	nonceB64 := base64.StdEncoding.EncodeToString(nonce)
	ciphertextB64 := base64.StdEncoding.EncodeToString(ciphertext)

	// Format: key_id|nonce|encrypted_data
	return fmt.Sprintf("%s|%s|%s", targetKeyID, nonceB64, ciphertextB64), nil
}

// Decrypt decrypts ciphertext using the key specified in the data
func (db *DB) Decrypt(encryptedData string) (string, error) {
	if encryptedData == "" {
		return "", nil
	}

	// Parse format: key_id|nonce|encrypted_data
	parts := strings.SplitN(encryptedData, "|", 3)
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid encrypted data format")
	}

	keyID := parts[0]
	nonceB64 := parts[1]
	ciphertextB64 := parts[2]

	// Get key
	key, exists := db.keys[keyID]
	if !exists {
		return "", fmt.Errorf("encryption key '%s' not found. Available: %v", keyID, db.GetKeyIDs())
	}

	// Decode from base64
	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode nonce: %w", err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode ciphertext: %w", err)
	}

	// Decrypt
	plaintext, err := key.cipher.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return string(plaintext), nil
}

// GetKeyIDs returns all available key IDs
func (db *DB) GetKeyIDs() []string {
	ids := make([]string, 0, len(db.keys))
	for id := range db.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// GetDefaultKeyID returns the default key ID
func (db *DB) GetDefaultKeyID() string {
	return db.defaultKey
}

// DB returns the underlying ORM database
// For Bun: returns *BunDB (with wrapped queries)
// For go-pg: returns *GoPgDB (with wrapped queries)
func (db *DB) DB() interface{} {
	return db.underlying
}

// GetKeyIDFromEncryptedData extracts key_id from encrypted data
func (db *DB) GetKeyIDFromEncryptedData(encryptedData string) (string, error) {
	if encryptedData == "" {
		return "", nil
	}

	parts := strings.SplitN(encryptedData, "|", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid encrypted data format")
	}

	return parts[0], nil
}
