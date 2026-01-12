package internal

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
)

// Encrypt encrypts plaintext with the specified key (or default if not specified)
func (g *GovaultDB) Encrypt(plaintext string, keyID ...string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	// Determine which key to use
	targetKeyID := g.defaultKey
	if len(keyID) > 0 && keyID[0] != "" {
		targetKeyID = keyID[0]
	}

	key, exists := g.keys[targetKeyID]
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
func (g *GovaultDB) Decrypt(encryptedData string) (string, error) {
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
	key, exists := g.keys[keyID]
	if !exists {
		return "", fmt.Errorf("encryption key '%s' not found. Available: %v", keyID, g.GetKeyIDs())
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

// DecryptRecursive handles decryption recursively
func (g *GovaultDB) DecryptRecursive(value interface{}) error {
	if value == nil {
		return nil
	}

	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	// Handle slice
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Ptr {
				// Recurse into ptr element
				if !elem.IsNil() {
					if err := g.DecryptRecursive(elem.Interface()); err != nil {
						return err
					}
				}
			} else if elem.Kind() == reflect.Struct {
				// If strictly a struct, check if addressable
				if elem.CanAddr() {
					if err := g.DecryptRecursive(elem.Addr().Interface()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// Handle single struct
	if val.Kind() == reflect.Struct {
		typ := val.Type()
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			fieldType := typ.Field(i)

			if !field.CanSet() {
				continue
			}

			// Decrypt if tagged
			if fieldType.Tag.Get("encrypted") == "true" {
				if field.Kind() == reflect.String {
					ciphertext := field.String()
					if ciphertext != "" && strings.Contains(ciphertext, "|") {
						decrypted, err := g.Decrypt(ciphertext)
						if err != nil {
							return fmt.Errorf("failed to decrypt field %s: %w", fieldType.Name, err)
						}
						field.SetString(decrypted)
					}
				}
			} else {
				// Recurse for nested structs/slices
				if field.Kind() == reflect.Struct {
					if field.CanAddr() {
						if err := g.DecryptRecursive(field.Addr().Interface()); err != nil {
							return err
						}
					}
				} else if field.Kind() == reflect.Ptr {
					if !field.IsNil() {
						if err := g.DecryptRecursive(field.Interface()); err != nil {
							return err
						}
					}
				} else if field.Kind() == reflect.Slice {
					// We need to pass the slice itself
					if field.CanAddr() {
						if err := g.DecryptRecursive(field.Addr().Interface()); err != nil {
							return err
						}
					} else {
						// Slice field value is a slice header, we can index it directly?
					}
				}
			}
		}
	}

	return nil
}
