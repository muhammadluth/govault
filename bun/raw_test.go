package bun_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBunRawQueryEncryptionDecryption(t *testing.T) {
	db, govaultDB, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Raw SQL INSERT with encryption", func(t *testing.T) {
		// Encrypt email before inserting
		rawQuery := db.NewRaw("SELECT 1") // dummy query to get access to EncryptValue
		encryptedEmail, err := rawQuery.EncryptValue("rawinsert@example.com")
		require.NoError(t, err)

		encryptedPhone, err := rawQuery.EncryptValue("+62811119999")
		require.NoError(t, err)

		// Insert using raw SQL with encrypted values
		_, err = db.NewRaw(
			"INSERT INTO test_users (name, email, phone) VALUES (?, ?, ?) RETURNING id",
			"Raw Insert Test",
			encryptedEmail,
			encryptedPhone,
		).Exec(ctx)

		require.NoError(t, err)

		// Verify the data is encrypted in database
		var rawEmail string
		err = db.NewRaw("SELECT email FROM test_users WHERE name = ?", "Raw Insert Test").
			Scan(ctx, &rawEmail)
		require.NoError(t, err)
		assert.Contains(t, rawEmail, "|", "Email should be encrypted in database")
	})

	t.Run("Raw SQL SELECT with decryption", func(t *testing.T) {
		// Insert test data with encrypted fields
		user := &TestUser{
			Name:  "Raw Select Test",
			Email: "rawselect@example.com",
			Phone: "+62822229999",
		}
		db.NewInsert().Model(user).Exec(ctx)

		// Select using raw SQL and verify decryption
		var retrieved TestUser
		err := db.NewRaw(
			"SELECT id, name, email, phone, address FROM test_users WHERE id = ?",
			user.ID,
		).Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, "rawselect@example.com", retrieved.Email, "Email should be decrypted")
		assert.Equal(t, "+62822229999", retrieved.Phone, "Phone should be decrypted")
		assert.NotContains(t, retrieved.Email, "|", "Email should not contain pipe after decryption")
	})

	t.Run("Raw SQL SELECT multiple rows with decryption", func(t *testing.T) {
		// Insert multiple test users
		users := []*TestUser{
			{Name: "Raw1", Email: "raw1@example.com", Phone: "+62833339991"},
			{Name: "Raw2", Email: "raw2@example.com", Phone: "+62833339992"},
		}
		for _, u := range users {
			db.NewInsert().Model(u).Exec(ctx)
		}

		// Select multiple using raw SQL
		var retrieved []TestUser
		err := db.NewRaw(
			"SELECT id, name, email, phone FROM test_users WHERE name LIKE ? ORDER BY name",
			"Raw%",
		).Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(retrieved), 2)

		// Verify all emails are decrypted
		for _, u := range retrieved {
			assert.NotContains(t, u.Email, "|", "Email should be decrypted")
			assert.Contains(t, u.Email, "@", "Email should be valid")
		}
	})

	t.Run("Raw SQL UPDATE with encryption", func(t *testing.T) {
		// Insert test user
		user := &TestUser{
			Name:  "Raw Update Test",
			Email: "rawupdate@example.com",
			Phone: "+62844449999",
		}
		db.NewInsert().Model(user).Exec(ctx)

		// Encrypt new email
		rawQuery := db.NewRaw("SELECT 1")
		newEncryptedEmail, err := rawQuery.EncryptValue("updated_raw@example.com")
		require.NoError(t, err)

		// Update using raw SQL
		_, err = db.NewRaw(
			"UPDATE test_users SET email = ? WHERE id = ?",
			newEncryptedEmail,
			user.ID,
		).Exec(ctx)
		require.NoError(t, err)

		// Verify update with decryption
		var updated TestUser
		err = db.NewRaw(
			"SELECT id, name, email FROM test_users WHERE id = ?",
			user.ID,
		).Scan(ctx, &updated)

		require.NoError(t, err)
		assert.Equal(t, "updated_raw@example.com", updated.Email)
	})

	t.Run("Raw SQL with WithKey", func(t *testing.T) {
		// Use specific key for encryption
		rawQuery := db.NewRaw("SELECT 1").WithKey("2")
		encryptedEmail, err := rawQuery.EncryptValue("withkey@example.com")
		require.NoError(t, err)

		// Verify key ID is in encrypted value
		assert.True(t, strings.HasPrefix(encryptedEmail, "2|"), "Should use key 2")

		// Decrypt to verify
		decrypted, err := govaultDB.Decrypt(encryptedEmail)
		require.NoError(t, err)
		assert.Equal(t, "withkey@example.com", decrypted)
	})

	t.Run("Raw SQL with empty encrypted value", func(t *testing.T) {
		rawQuery := db.NewRaw("SELECT 1")
		encryptedEmpty, err := rawQuery.EncryptValue("")
		require.NoError(t, err)
		assert.Equal(t, "", encryptedEmpty, "Empty string should remain empty")
	})

	t.Run("Raw SQL SELECT without encrypted fields", func(t *testing.T) {
		// Insert user
		user := &TestUser{
			Name:    "Plain Raw",
			Email:   "plain@example.com",
			Phone:   "+62855559999",
			Address: "Test Address",
		}
		db.NewInsert().Model(user).Exec(ctx)

		// Select only non-encrypted field
		var name string
		err := db.NewRaw(
			"SELECT name FROM test_users WHERE id = ?",
			user.ID,
		).Scan(ctx, &name)

		require.NoError(t, err)
		assert.Equal(t, "Plain Raw", name)
	})
}

func TestBunRawExotic(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("NewRaw", func(t *testing.T) {
		var count int
		err := db.NewRaw("SELECT COUNT(*) FROM test_users").Scan(ctx, &count)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, 0)
	})
}
