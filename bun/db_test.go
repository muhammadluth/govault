package bun_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/muhammadluth/govault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestBunAdapter(t *testing.T) {
	t.Run("adapter get name", func(t *testing.T) {
		openDB := sql.OpenDB(pgdriver.NewConnector(
			pgdriver.WithNetwork("tcp"),
			pgdriver.WithAddr("localhost:5433"),
			pgdriver.WithUser("postgres"),
			pgdriver.WithPassword("Admin123!"),
			pgdriver.WithDatabase("postgres"),
			pgdriver.WithApplicationName("playground"),
			pgdriver.WithTLSConfig(nil),
			pgdriver.WithDialTimeout(5*time.Second),
		))
		defer openDB.Close()

		bunDB := bun.NewDB(openDB, pgdialect.New())
		govaultDB, err := govault.New(govault.Config{
			AdapterName: govault.AdapterNameBun,
			BunDB:       bunDB,
			Keys: map[string][]byte{
				"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
			},
			DefaultKeyID: "1",
		})
		require.NoError(t, err)

		db := govaultDB.BunDB()
		assert.NotNil(t, db)
		assert.NotNil(t, govaultDB)
	})
}

func TestBunDBWithKey(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("with key usage", func(t *testing.T) {
		user := &TestUser{
			Name:  "With Key User",
			Email: "withkey@example.com",
			Phone: "+62844444444",
		}

		// Insert with specific key
		db.WithKey("2").NewInsert().Model(user).Exec(ctx)

		// Verify key used
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Email         string `bun:"email"`
		}

		var raw RawUser
		err := db.NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		assert.Contains(t, raw.Email, "|")
		parts := strings.Split(raw.Email, "|")
		assert.Equal(t, "2", parts[0])
	})
}

func TestBunMultipleKeys(t *testing.T) {
	db, govaultDB, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("read data encrypted with different keys", func(t *testing.T) {
		// Insert with key 1
		user1 := &TestUser{
			Name:  "User Key 1",
			Email: "user1@example.com",
			Phone: "+62811111111",
		}
		db.NewInsert().WithKey("1").Model(user1).Exec(ctx)

		// Insert with key 2
		user2 := &TestUser{
			Name:  "User Key 2",
			Email: "user2@example.com",
			Phone: "+62822222222",
		}
		db.NewInsert().WithKey("2").Model(user2).Exec(ctx)

		// Read both (should work because we have both keys)
		var users []TestUser
		err := db.NewSelect().
			Model(&users).
			Where("name IN (?)", bun.In([]string{"User Key 1", "User Key 2"})).
			Order("id ASC").
			Scan(ctx, &users)

		require.NoError(t, err)
		assert.Equal(t, 2, len(users))
		assert.Equal(t, "user1@example.com", users[0].Email)
		assert.Equal(t, "user2@example.com", users[1].Email)

		// Verify keys used
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Email         string `bun:"email"`
		}

		var raw1, raw2 RawUser
		db.NewSelect().Model(&raw1).Where("id = ?", user1.ID).Scan(ctx, &raw1)
		db.NewSelect().Model(&raw2).Where("id = ?", user2.ID).Scan(ctx, &raw2)

		keyID1, _ := govaultDB.GetKeyIDFromEncryptedData(raw1.Email)
		keyID2, _ := govaultDB.GetKeyIDFromEncryptedData(raw2.Email)

		assert.Equal(t, "1", keyID1)
		assert.Equal(t, "2", keyID2)
	})
}

func TestGetKeyIDs(t *testing.T) {
	_, db, cleanup := setupTestDB(t)
	defer cleanup()

	// Test GetKeyIDs returns all key IDs sorted
	ids := db.GetKeyIDs()
	expected := []string{"1", "2", "3"}
	assert.Equal(t, expected, ids)
}

func TestGetDefaultKeyID(t *testing.T) {
	_, db, cleanup := setupTestDB(t)
	defer cleanup()

	// Test GetDefaultKeyID returns the default key ID
	defaultID := db.GetDefaultKeyID()
	assert.Equal(t, "3", defaultID)
}

func TestBunEncryptionErrorHandling(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("encrypt model with invalid key", func(t *testing.T) {
		user := &TestUser{
			Name:  "Error User",
			Email: "error@example.com",
			Phone: "+62855555555",
		}

		// This should panic due to invalid key
		assert.Panics(t, func() {
			db.WithKey("invalid").NewInsert().Model(user).Exec(ctx)
		})
	})

	t.Run("update model with invalid key", func(t *testing.T) {
		user := &TestUser{
			Name:  "Update Error User",
			Email: "updateerror@example.com",
			Phone: "+62866666666",
		}
		db.NewInsert().Model(user).Exec(ctx)

		user.Email = "newemail@example.com"

		// This should panic due to invalid key
		assert.Panics(t, func() {
			db.WithKey("invalid").NewUpdate().Model(user).WherePK().Exec(ctx)
		})
	})
}

func TestBunEncryptDecryptEdgeCases(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("encrypt non-string field", func(t *testing.T) {
		// Create table for int test
		_, err := db.DB.NewCreateTable().
			Model((*TestUserWithInt)(nil)).
			IfNotExists().
			Exec(ctx)
		require.NoError(t, err)
		defer db.DB.NewDropTable().Model((*TestUserWithInt)(nil)).IfExists().Exec(ctx)

		user := &TestUserWithInt{
			Name:  "Int User",
			Age:   25,
			Email: "int@example.com",
		}

		// This should work - only string fields with encrypted tag are processed
		_, err = db.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)
		assert.NotZero(t, user.ID)
	})

	t.Run("decrypt invalid encrypted data", func(t *testing.T) {
		user := &TestUser{
			Name:  "Invalid Decrypt",
			Email: "invalid|data|here",
			Phone: "+62877777777",
		}

		// Manually insert corrupted data
		_, err := db.DB.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		// Try to select - should return error
		var retrieved TestUser
		err = db.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx, &retrieved)

		assert.Error(t, err)
	})
}

func TestDecryptValueExotic(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("Decrypt slice of pointers", func(t *testing.T) {
		user := &TestUser{Name: "SlicePointer", Email: "ptr@example.com"}
		db.NewInsert().Model(user).Exec(ctx)

		var users []*TestUser
		err := db.NewSelect().Model(&users).Where("name = ?", "SlicePointer").Scan(ctx, &users)
		require.NoError(t, err)
		require.NotEmpty(t, users)
		assert.Equal(t, "ptr@example.com", users[0].Email)
	})

	t.Run("Decrypt slice of structs", func(t *testing.T) {
		user := &TestUser{Name: "SliceStruct", Email: "struct@example.com"}
		db.NewInsert().Model(user).Exec(ctx)

		var users []TestUser
		err := db.NewSelect().Model(&users).Where("name = ?", "SliceStruct").Scan(ctx, &users)
		require.NoError(t, err)
		require.NotEmpty(t, users)
		assert.Equal(t, "struct@example.com", users[0].Email)
	})
}

func TestBunGeneralDDL(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("CreateTable and DropTable", func(t *testing.T) {
		type TempTable struct {
			bun.BaseModel `bun:"table:temp_test_adv"`
			ID            int64  `bun:"id,pk,autoincrement"`
			Name          string `bun:"name"`
		}

		_, err := db.NewCreateTable().
			Model((*TempTable)(nil)).
			IfNotExists().
			Exec(ctx)

		require.NoError(t, err)

		// Cleanup
		db.NewDropTable().Model((*TempTable)(nil)).IfExists().Exec(ctx)
	})

	t.Run("CreateIndex and DropIndex", func(t *testing.T) {
		_, err := db.NewCreateIndex().Model((*TestUser)(nil)).Index("idx_name").Column("name").Exec(ctx)
		require.NoError(t, err)

		_, err = db.NewDropIndex().Index("idx_name").Exec(ctx)
		require.NoError(t, err)
	})

	t.Run("TruncateTable", func(t *testing.T) {
		_, err := db.NewTruncateTable().Model((*TestUser)(nil)).Exec(ctx)
		require.NoError(t, err)
	})
}

func TestBunValues(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	user := &TestUser{Name: "ValuesTest"}
	q := db.NewValues(user)
	assert.NotNil(t, q)
}

func TestBunGlobalQueryErrorHandling(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("Query with Err", func(t *testing.T) {
		var users []TestUser
		err := db.NewSelect().
			Model(&users).
			Err(assert.AnError).
			Scan(ctx, &users)

		assert.Error(t, err)
	})
}

func TestBunManualTransaction(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("Commit Transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		user := &TestUser{Name: "Tx User", Email: "tx@example.com"}
		_, err = tx.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// Verify user exists
		exists, err := db.NewSelect().Model((*TestUser)(nil)).Where("name = ?", "Tx User").Exists(ctx)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Rollback Transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		user := &TestUser{Name: "Rollback User", Email: "rollback@example.com"}
		_, err = tx.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		// Verify user does not exist
		exists, err := db.NewSelect().Model((*TestUser)(nil)).Where("name = ?", "Rollback User").Exists(ctx)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("BeginTx with Options", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		require.NoError(t, err)
		require.NotNil(t, tx)
		tx.Rollback()
	})
}

func TestBunTxQueryMethods(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	// Insert via Tx
	user := &TestUser{Name: "TxQuery User", Email: "txquery@example.com"}
	_, err = tx.NewInsert().Model(user).Exec(ctx)
	require.NoError(t, err)

	// Select via Tx
	var retrieved TestUser
	err = tx.NewSelect().Model(&retrieved).Where("id = ?", user.ID).Scan(ctx, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, "txquery@example.com", retrieved.Email)

	// Update via Tx
	_, err = tx.NewUpdate().Model(user).Set("name = ?", "Updated Tx").WherePK().Exec(ctx)
	require.NoError(t, err)

	// Delete via Tx
	_, err = tx.NewDelete().Model(user).WherePK().Exec(ctx)
	require.NoError(t, err)

	// Raw via Tx
	var count int
	err = tx.NewRaw("SELECT 1").Scan(ctx, &count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
