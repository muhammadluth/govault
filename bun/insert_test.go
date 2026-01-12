// Package govault - Bun adapter insert query tests
package bun_test

import (
	"context"
	"strings"
	"testing"

	gb "github.com/muhammadluth/govault/bun"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestBunInsert(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("insert single user", func(t *testing.T) {
		user := &TestUser{
			Name:    "John Doe",
			Email:   "john@example.com",
			Phone:   "+62812345678",
			Address: "Jakarta",
		}

		_, err := db.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)
		assert.NotZero(t, user.ID)
	})

	t.Run("insert encrypts data in database", func(t *testing.T) {
		user := &TestUser{
			Name:  "Jane Doe",
			Email: "jane@example.com",
			Phone: "+62898765432",
		}

		_, err := db.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		// Check raw data in database
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Email         string `bun:"email"`
			Phone         string `bun:"phone"`
		}

		var raw RawUser
		err = db.NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Email and Phone should be encrypted (contain |)
		assert.Contains(t, raw.Email, "|")
		assert.Contains(t, raw.Phone, "|")
		assert.Equal(t, 2, strings.Count(raw.Email, "|"))
		assert.Equal(t, 2, strings.Count(raw.Phone, "|"))
	})
}

func TestBunNonEncryptedFields(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("non-encrypted fields remain plaintext", func(t *testing.T) {
		user := &TestUser{
			Name:    "John Doe",
			Email:   "john@example.com",
			Phone:   "+62812345678",
			Address: "Jakarta, Indonesia",
		}

		db.NewInsert().Model(user).Exec(ctx)

		// Check raw data
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Name          string `bun:"name"`
			Address       string `bun:"address"`
			Email         string `bun:"email"`
		}

		var raw RawUser
		err := db.NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Name and Address should be plaintext
		assert.Equal(t, "John Doe", raw.Name)
		assert.Equal(t, "Jakarta, Indonesia", raw.Address)

		// Email should be encrypted
		assert.Contains(t, raw.Email, "|")
	})
}

func TestBunEmptyFields(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty encrypted fields", func(t *testing.T) {
		user := &TestUser{
			Name:  "User with empty fields",
			Email: "",
			Phone: "",
		}

		_, err := db.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		// Select
		var retrieved TestUser
		err = db.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx)

		require.NoError(t, err)
		assert.Equal(t, "", retrieved.Email)
		assert.Equal(t, "", retrieved.Phone)
	})
}

func TestBunBulkInsert(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("bulk insert users", func(t *testing.T) {
		users := []TestUser{
			{Name: "User 1", Email: "user1@example.com", Phone: "+62811111111"},
			{Name: "User 2", Email: "user2@example.com", Phone: "+62822222222"},
			{Name: "User 3", Email: "user3@example.com", Phone: "+62833333333"},
		}

		_, err := db.NewInsert().Model(&users).Exec(ctx)
		require.NoError(t, err)

		// Verify all inserted and encrypted
		var retrieved []TestUser
		err = db.NewSelect().
			Model(&retrieved).
			Where("name LIKE ?", "User %").
			Order("name ASC").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, 3, len(retrieved))

		for i, u := range retrieved {
			assert.Equal(t, users[i].Email, u.Email)
			assert.NotContains(t, u.Email, "|")
		}
	})
}

func TestBunPrivateFieldEncryption(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("private encrypted field", func(t *testing.T) {
		// Create table
		_, err := db.DB.NewCreateTable().
			Model((*TestUserWithPrivate)(nil)).
			IfNotExists().
			Exec(ctx)
		require.NoError(t, err)
		defer db.DB.NewDropTable().Model((*TestUserWithPrivate)(nil)).IfExists().Exec(ctx)

		user := &TestUserWithPrivate{
			Name:  "Private User",
			email: "private@example.com", // private field
			Email: "public@example.com",
		}

		// Insert - private field should not be encrypted since !field.CanSet()
		_, err = db.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		// Check raw data
		type RawPrivateUser struct {
			bun.BaseModel `bun:"table:test_users_private"`
			ID            int64  `bun:"id"`
			Name          string `bun:"name"`
			Email         string `bun:"email_public"`
		}

		var raw RawPrivateUser
		err = db.DB.NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Private field is not in DB schema, public Email should be plaintext
		assert.Equal(t, "Private User", raw.Name)
		assert.Equal(t, "public@example.com", raw.Email)
	})
}

func TestBunInsertAdvanced(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("Insert with Returning", func(t *testing.T) {
		user := &TestUser{
			Name:  "Returning Test",
			Email: "returning@example.com",
			Phone: "+62855555565",
		}

		err := db.NewInsert().
			Model(user).
			Returning("id").
			Scan(ctx, user)

		require.NoError(t, err)
		assert.NotZero(t, user.ID)
	})
}

func TestBunInsertApply(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	user := &TestUser{Name: "Insert Apply", Email: "insertapply@example.com", Phone: "+62811119992"}

	applyFunc := func(q *gb.BunInsertQuery) *gb.BunInsertQuery {
		return q.Column("name", "email", "phone")
	}

	_, err := db.NewInsert().
		Model(user).
		Apply(applyFunc).
		Exec(ctx)

	require.NoError(t, err)
}

func TestBunInsertExotic(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("Insert Exotic", func(t *testing.T) {
		user := &TestUser{Name: "InsertExotic"}

		// Test Conn
		db.NewInsert().Model(user).Conn(db.DB).Exec(ctx)

		// Table and TableExpr
		db.NewInsert().Model(user).Table("test_users").TableExpr("test_users AS u").Exec(ctx)

		// ModelTableExpr
		db.NewInsert().Model(user).ModelTableExpr("test_users AS u").Exec(ctx)

		// ColumnExpr
		db.NewInsert().Model(user).ColumnExpr("name").Exec(ctx)

		// WhereOr
		db.NewInsert().Model(user).Where("1=1").WhereOr("2=2").Exec(ctx)

		// Ignore and Replace
		db.NewInsert().Model(user).Ignore().Exec(ctx)
		db.NewInsert().Model(user).Replace().Exec(ctx)

		// Comment
		db.NewInsert().Model(user).Comment("test").Exec(ctx)

		// CTEs
		withQ := db.NewSelect().Model((*TestUser)(nil))
		db.NewInsert().Model(user).With("cte", withQ).Exec(ctx)
		db.NewInsert().Model(user).WithRecursive("cte", withQ).Exec(ctx)
	})

	t.Run("BunInsertQuery Exotic 2", func(t *testing.T) {
		user := &TestUser{Name: "InsertExotic2", Email: "exotic2@example.com"}

		// ExcludeColumn
		_, err := db.NewInsert().Model(user).ExcludeColumn("phone").Exec(ctx)
		require.NoError(t, err)

		// Value
		_, err = db.NewInsert().Model(&TestUser{Name: "ValueInsert"}).Value("email", "?", "val|key|nonce").Exec(ctx)
		require.NoError(t, err)

		// On Conflict
		_, err = db.NewInsert().Model(user).On("CONFLICT (id) DO UPDATE SET name = EXCLUDED.name").Exec(ctx)
		require.NoError(t, err)
	})
}
