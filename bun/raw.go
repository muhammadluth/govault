// Package govault - Bun adapter raw query implementation
package bun

import (
	"context"
	"database/sql"

	"github.com/muhammadluth/govault/internal"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
)

// BunRawQuery wraps bun.RawQuery with encryption/decryption support
type BunRawQuery struct {
	*bun.RawQuery
	govault *internal.GovaultDB
	keyID   string
}

func (q *BunRawQuery) Conn(db bun.IConn) *BunRawQuery {
	q.RawQuery.Conn(db)
	return q
}

func (q *BunRawQuery) Err(err error) *BunRawQuery {
	q.RawQuery.Err(err)
	return q
}

// Exec executes the raw query
func (q *BunRawQuery) Exec(ctx context.Context, dest ...any) (sql.Result, error) {
	res, err := q.RawQuery.Exec(ctx, dest...)
	if err != nil {
		return res, err
	}

	// If destinations are provided (using RETURNING), attempt to decrypt
	if len(dest) > 0 {
		for _, d := range dest {
			if err := q.govault.DecryptRecursive(d); err != nil {
				// We log or return error?
				// Since query succeeded, we should probably return the error as it affects the data integrity for the caller.
				return res, err
			}
		}
	}
	return res, nil
}

// Scan executes the raw query and scans results
// If dest is a struct with encrypted fields, they will be decrypted
func (q *BunRawQuery) Scan(ctx context.Context, dest ...any) error {
	err := q.RawQuery.Scan(ctx, dest...)
	if err != nil {
		return err
	}

	// Attempt to decrypt if dest contains encrypted fields
	for _, d := range dest {
		if err := q.govault.DecryptRecursive(d); err != nil {
			return err
		}
	}

	return nil
}

// Comment adds a comment to the query, wrapped by /* ... */.
func (q *BunRawQuery) Comment(comment string) *BunRawQuery {
	q.RawQuery.Comment(comment)
	return q
}

func (q *BunRawQuery) AppendQuery(gen schema.QueryGen, b []byte) ([]byte, error) {
	return q.RawQuery.AppendQuery(gen, b)
}

func (q *BunRawQuery) Operation() string {
	return q.RawQuery.Operation()
}

func (q *BunRawQuery) String() string {
	return q.RawQuery.String()
}

// WithKey sets the encryption key for this raw query
func (q *BunRawQuery) WithKey(keyID string) *BunRawQuery {
	q.keyID = keyID
	return q
}

// EncryptValue encrypts a single value for use in raw SQL
// Returns encrypted string in format: keyID|nonce|ciphertext
func (q *BunRawQuery) EncryptValue(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	if q.keyID != "" {
		return q.govault.Encrypt(plaintext, q.keyID)
	}
	return q.govault.Encrypt(plaintext)
}
