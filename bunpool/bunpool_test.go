package bunpool_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/muhammadluth/govault"
	"github.com/muhammadluth/govault/bunpool"
)

type TestUser struct {
	bun.BaseModel `bun:"table:test_users,alias:u"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email,notnull" encrypted:"true"`
	Phone         string `bun:"phone" encrypted:"true"`
	Address       string `bun:"address"`
}

func setupTestDB(t *testing.T) (*bunpool.Pool, *govault.Encryptor, func()) {
	dsn := "postgres://postgres:Admin123!@localhost:5433/postgres?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	// Create pool
	pool := bunpool.NewPool(sqldb, pgdialect.New())

	// Create encryptor
	keysMap := map[string][]byte{
		"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
		"2": []byte("e778dc27-9b04-44c3-a862-feba061c"),
	}

	encryptor, err := govault.NewWithKeys(keysMap, "2", pool)
	require.NoError(t, err)

	pool.SetEncryptor(encryptor)

	// Create table
	ctx := context.Background()
	_, err = pool.DB().NewCreateTable().
		Model((*TestUser)(nil)).
		IfNotExists().
		Exec(ctx)
	require.NoError(t, err)

	// Clean table
	_, err = pool.DB().NewDelete().Model((*TestUser)(nil)).Where("1=1").Exec(ctx)
	require.NoError(t, err)

	cleanup := func() {
		pool.DB().NewDropTable().Model((*TestUser)(nil)).IfExists().Exec(ctx)
		sqldb.Close()
	}

	return pool, encryptor, cleanup
}

func TestBunInsert(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("insert single user", func(t *testing.T) {
		user := &TestUser{
			Name:    "John Doe",
			Email:   "john@example.com",
			Phone:   "+62812345678",
			Address: "Jakarta",
		}

		_, err := pool.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)
		assert.NotZero(t, user.ID)
	})

	t.Run("insert encrypts data in database", func(t *testing.T) {
		user := &TestUser{
			Name:  "Jane Doe",
			Email: "jane@example.com",
			Phone: "+62898765432",
		}

		_, err := pool.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		// Check raw data in database
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Email         string `bun:"email"`
			Phone         string `bun:"phone"`
		}

		var raw RawUser
		err = pool.DB().NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Email and Phone should be encrypted (contain |)
		assert.Contains(t, raw.Email, "|")
		assert.Contains(t, raw.Phone, "|")
		assert.Equal(t, 2, strings.Count(raw.Email, "|"))
		assert.Equal(t, 2, strings.Count(raw.Phone, "|"))
	})
}

func TestBunSelect(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("select single user", func(t *testing.T) {
		// Insert
		user := &TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Phone: "+62812345678",
		}
		pool.NewInsert().Model(user).Exec(ctx)

		// Select
		var retrieved TestUser
		err := pool.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx, &retrieved)

		fmt.Println("retrieved", retrieved)

		require.NoError(t, err)
		assert.Equal(t, user.Name, retrieved.Name)
		assert.Equal(t, "john@example.com", retrieved.Email)
		assert.Equal(t, "+62812345678", retrieved.Phone)
	})

	t.Run("select multiple users", func(t *testing.T) {
		// Insert multiple
		users := []*TestUser{
			{Name: "Alice", Email: "alice@example.com", Phone: "+62811111111"},
			{Name: "Bob", Email: "bob@example.com", Phone: "+62822222222"},
			{Name: "Charlie", Email: "charlie@example.com", Phone: "+62833333333"},
		}

		for _, u := range users {
			pool.NewInsert().Model(u).Exec(ctx)
		}

		// Select all
		var retrieved []TestUser
		err := pool.NewSelect().
			Model(&retrieved).
			Order("id ASC").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(retrieved), 3)

		// Check decryption
		for _, u := range retrieved {
			assert.NotContains(t, u.Email, "|", "Email should be decrypted")
			assert.NotContains(t, u.Phone, "|", "Phone should be decrypted")
			assert.Contains(t, u.Email, "@example.com")
		}
	})

	t.Run("select with WHERE clause", func(t *testing.T) {
		user := &TestUser{
			Name:  "Test User",
			Email: "testuser@example.com",
			Phone: "+62899999999",
		}
		pool.NewInsert().Model(user).Exec(ctx)

		var retrieved TestUser
		err := pool.NewSelect().
			Model(&retrieved).
			Where("name = ?", "Test User").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, "testuser@example.com", retrieved.Email)
	})

	t.Run("select with LIMIT and OFFSET", func(t *testing.T) {
		var users []TestUser
		err := pool.NewSelect().
			Model(&users).
			Order("id ASC").
			Limit(2).
			Offset(1).
			Scan(ctx, &users)

		require.NoError(t, err)
		assert.LessOrEqual(t, len(users), 2)
	})
}

func TestBunUpdate(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("update user", func(t *testing.T) {
		// Insert
		user := &TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Phone: "+62812345678",
		}
		pool.NewInsert().Model(user).Exec(ctx)

		// Update
		user.Email = "newemail@example.com"
		user.Phone = "+62899999999"

		_, err := pool.NewUpdate().
			Model(user).
			WherePK().
			Exec(ctx)
		require.NoError(t, err)

		// Verify
		var retrieved TestUser
		err = pool.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, "newemail@example.com", retrieved.Email)
		assert.Equal(t, "+62899999999", retrieved.Phone)
	})

	t.Run("update re-encrypts with active key", func(t *testing.T) {
		user := &TestUser{
			Name:  "Jane Doe",
			Email: "jane@example.com",
			Phone: "+62898765432",
		}
		pool.NewInsert().Model(user).Exec(ctx)

		user.Email = "updated@example.com"
		pool.NewUpdate().Model(user).WherePK().Exec(ctx)

		// Check raw data
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Email         string `bun:"email"`
		}

		var raw RawUser
		err := pool.DB().NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Should be encrypted
		assert.Contains(t, raw.Email, "|")
		parts := strings.Split(raw.Email, "|")
		assert.Equal(t, "2", parts[0]) // Active key
	})
}

func TestBunDelete(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("delete user", func(t *testing.T) {
		user := &TestUser{
			Name:  "To Delete",
			Email: "delete@example.com",
			Phone: "+62800000000",
		}
		pool.NewInsert().Model(user).Exec(ctx)

		// Delete
		_, err := pool.NewDelete().
			Model(user).
			WherePK().
			Exec(ctx)
		require.NoError(t, err)

		// Verify deleted
		var retrieved TestUser
		err = pool.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx)

		assert.Error(t, err) // Should not find
	})
}

func TestBunMultipleKeys(t *testing.T) {
	pool, encryptor, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("read data encrypted with different keys", func(t *testing.T) {
		// Create encryptor with key 1
		keysMap1 := map[string][]byte{
			"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
		}
		enc1, _ := govault.NewWithKeys(keysMap1, "1", pool)
		pool.SetEncryptor(enc1)

		// Insert with key 1
		user1 := &TestUser{
			Name:  "User Key 1",
			Email: "user1@example.com",
			Phone: "+62811111111",
		}
		pool.NewInsert().Model(user1).Exec(ctx)

		// Switch to key 2
		keysMap2 := map[string][]byte{
			"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
			"2": []byte("e778dc27-9b04-44c3-a862-feba061c"),
		}
		enc2, _ := govault.NewWithKeys(keysMap2, "2", pool)
		pool.SetEncryptor(enc2)

		// Insert with key 2
		user2 := &TestUser{
			Name:  "User Key 2",
			Email: "user2@example.com",
			Phone: "+62822222222",
		}
		pool.NewInsert().Model(user2).Exec(ctx)

		// Read both (should work because we have both keys)
		var users []TestUser
		err := pool.NewSelect().
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
		pool.DB().NewSelect().Model(&raw1).Where("id = ?", user1.ID).Scan(ctx, &raw1)
		pool.DB().NewSelect().Model(&raw2).Where("id = ?", user2.ID).Scan(ctx, &raw2)

		keyID1, _ := encryptor.GetKeyIDFromEncryptedData(raw1.Email)
		keyID2, _ := encryptor.GetKeyIDFromEncryptedData(raw2.Email)

		assert.Equal(t, "1", keyID1)
		assert.Equal(t, "2", keyID2)
	})
}

func TestBunNonEncryptedFields(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("non-encrypted fields remain plaintext", func(t *testing.T) {
		user := &TestUser{
			Name:    "John Doe",
			Email:   "john@example.com",
			Phone:   "+62812345678",
			Address: "Jakarta, Indonesia",
		}

		pool.NewInsert().Model(user).Exec(ctx)

		// Check raw data
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Name          string `bun:"name"`
			Address       string `bun:"address"`
			Email         string `bun:"email"`
		}

		var raw RawUser
		err := pool.DB().NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Name and Address should be plaintext
		assert.Equal(t, "John Doe", raw.Name)
		assert.Equal(t, "Jakarta, Indonesia", raw.Address)

		// Email should be encrypted
		assert.Contains(t, raw.Email, "|")
	})
}

func TestBunEmptyFields(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty encrypted fields", func(t *testing.T) {
		user := &TestUser{
			Name:  "User with empty fields",
			Email: "",
			Phone: "",
		}

		_, err := pool.NewInsert().Model(user).Exec(ctx)
		require.NoError(t, err)

		// Select
		var retrieved TestUser
		err = pool.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx)

		require.NoError(t, err)
		assert.Equal(t, "", retrieved.Email)
		assert.Equal(t, "", retrieved.Phone)
	})
}

func TestBunBulkInsert(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("bulk insert users", func(t *testing.T) {
		users := []TestUser{
			{Name: "User 1", Email: "user1@example.com", Phone: "+62811111111"},
			{Name: "User 2", Email: "user2@example.com", Phone: "+62822222222"},
			{Name: "User 3", Email: "user3@example.com", Phone: "+62833333333"},
		}

		_, err := pool.NewInsert().Model(&users).Exec(ctx)
		require.NoError(t, err)

		// Verify all inserted and encrypted
		var retrieved []TestUser
		err = pool.NewSelect().
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

func TestBunCount(t *testing.T) {
	pool, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("count users", func(t *testing.T) {
		// Insert test users
		for i := 1; i <= 5; i++ {
			user := &TestUser{
				Name:  "Count Test User",
				Email: "count@example.com",
			}
			pool.NewInsert().Model(user).Exec(ctx)
		}

		count, err := pool.DB().NewSelect().
			Model((*TestUser)(nil)).
			Where("name = ?", "Count Test User").
			Count(ctx)

		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}
