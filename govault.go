package govault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Pool represents the interface for ORM-specific implementations
// Similar to redsync.Pool interface
type Pool interface {
	// GetName returns the name of the pool implementation (e.g., "bun", "go-pg")
	GetName() string
}

// Encryptor is the main encryption manager
// Similar to redsync.Redsync struct
type Encryptor struct {
	pools       []Pool
	keys        map[string]*EncryptionKey
	activeKeyID string
	activeKey   *EncryptionKey
}

// EncryptionKey represents a single encryption key with its ID
type EncryptionKey struct {
	ID     string
	Key    []byte
	cipher cipher.AEAD
}

// New creates a new Encryptor instance with the given pools
// Similar to redsync.New(pools...)
func New(pools ...Pool) (*Encryptor, error) {
	if len(pools) == 0 {
		return nil, fmt.Errorf("at least one pool is required")
	}

	keys, activeKeyID, err := loadKeysFromEnv()
	if err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("no encryption keys found in environment variables (ENCRYPTION_KEY_1, ENCRYPTION_KEY_2, etc)")
	}

	return &Encryptor{
		pools:       pools,
		keys:        keys,
		activeKeyID: activeKeyID,
		activeKey:   keys[activeKeyID],
	}, nil
}

// NewWithKeys creates a new Encryptor with manually provided keys
// Useful for testing or custom configurations
func NewWithKeys(keysMap map[string][]byte, activeKeyID string, pools ...Pool) (*Encryptor, error) {
	if len(pools) == 0 {
		return nil, fmt.Errorf("at least one pool is required")
	}

	if len(keysMap) == 0 {
		return nil, fmt.Errorf("no encryption keys provided")
	}

	if _, exists := keysMap[activeKeyID]; !exists {
		return nil, fmt.Errorf("active key ID %s not found in provided keys", activeKeyID)
	}

	keys := make(map[string]*EncryptionKey)
	for keyID, keyBytes := range keysMap {
		key, err := newEncryptionKey(keyID, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize key %s: %w", keyID, err)
		}
		keys[keyID] = key
	}

	return &Encryptor{
		pools:       pools,
		keys:        keys,
		activeKeyID: activeKeyID,
		activeKey:   keys[activeKeyID],
	}, nil
}

// loadKeysFromEnv loads encryption keys from environment variables
func loadKeysFromEnv() (map[string]*EncryptionKey, string, error) {
	keys := make(map[string]*EncryptionKey)
	keyNumbers := []int{}

	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "ENCRYPTION_KEY_") {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) != 2 {
				continue
			}

			envKey := parts[0]
			envValue := parts[1]

			numStr := strings.TrimPrefix(envKey, "ENCRYPTION_KEY_")
			num, err := strconv.Atoi(numStr)
			if err != nil {
				continue
			}

			keyBytes := []byte(envValue)
			if len(keyBytes) != 32 {
				return nil, "", fmt.Errorf("ENCRYPTION_KEY_%d must be 32 bytes, got %d bytes", num, len(keyBytes))
			}

			keyID := fmt.Sprintf("%d", num)
			key, err := newEncryptionKey(keyID, keyBytes)
			if err != nil {
				return nil, "", fmt.Errorf("failed to initialize ENCRYPTION_KEY_%d: %w", num, err)
			}

			keys[keyID] = key
			keyNumbers = append(keyNumbers, num)
		}
	}

	if len(keys) == 0 {
		return nil, "", fmt.Errorf("no ENCRYPTION_KEY_* found in environment")
	}

	sort.Ints(keyNumbers)
	activeKeyNum := keyNumbers[len(keyNumbers)-1]
	activeKeyID := fmt.Sprintf("%d", activeKeyNum)

	return keys, activeKeyID, nil
}

// newEncryptionKey creates a new EncryptionKey
func newEncryptionKey(keyID string, encryptionKey []byte) (*EncryptionKey, error) {
	if len(encryptionKey) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes for AES-256")
	}

	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &EncryptionKey{
		ID:     keyID,
		Key:    encryptionKey,
		cipher: aead,
	}, nil
}

// GetActiveKeyID returns the active key ID
func (e *Encryptor) GetActiveKeyID() string {
	return e.activeKeyID
}

// GetKeyIDs returns all available key IDs
func (e *Encryptor) GetKeyIDs() []string {
	ids := make([]string, 0, len(e.keys))
	for id := range e.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Encrypt encrypts plaintext with the active key
// Format: key_id|nonce|encrypted_data
func (e *Encryptor) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	cipher := e.activeKey.cipher
	nonce := make([]byte, cipher.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := cipher.Seal(nil, nonce, []byte(plaintext), nil)

	nonceB64 := base64.StdEncoding.EncodeToString(nonce)
	ciphertextB64 := base64.StdEncoding.EncodeToString(ciphertext)

	result := fmt.Sprintf("%s|%s|%s", e.activeKeyID, nonceB64, ciphertextB64)
	return result, nil
}

// Decrypt decrypts ciphertext using the appropriate key based on key_id in data
// Format: key_id|nonce|encrypted_data
func (e *Encryptor) Decrypt(encryptedData string) (string, error) {
	if encryptedData == "" {
		return "", nil
	}

	parts := strings.SplitN(encryptedData, "|", 3)
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid encrypted data format, expected: key_id|nonce|encrypted_data")
	}

	keyID := parts[0]
	nonceB64 := parts[1]
	ciphertextB64 := parts[2]

	key, exists := e.keys[keyID]
	if !exists {
		return "", fmt.Errorf("encryption key with ID '%s' not found. Available keys: %v", keyID, e.GetKeyIDs())
	}

	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode nonce: %w", err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode ciphertext: %w", err)
	}

	plaintext, err := key.cipher.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt with key %s: %w", keyID, err)
	}

	return string(plaintext), nil
}

// ReEncrypt re-encrypts data with the active key
func (e *Encryptor) ReEncrypt(encryptedData string) (string, error) {
	if encryptedData == "" {
		return "", nil
	}

	plaintext, err := e.Decrypt(encryptedData)
	if err != nil {
		return "", err
	}

	return e.Encrypt(plaintext)
}

// GetKeyIDFromEncryptedData extracts key_id from encrypted data
func (e *Encryptor) GetKeyIDFromEncryptedData(encryptedData string) (string, error) {
	if encryptedData == "" {
		return "", nil
	}

	parts := strings.SplitN(encryptedData, "|", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid encrypted data format")
	}

	return parts[0], nil
}
