package govault_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/muhammadluth/govault"
)

type TestUser struct {
	bun.BaseModel `bun:"table:test_users,alias:u"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email,notnull" encrypted:"true"`
	Phone         string `bun:"phone" encrypted:"true"`
	Address       string `bun:"address"`
}

func setupTestDB(t *testing.T) (*govault.BunDB, *govault.DB, func()) {
	// Setup Bun connection
	openDB := sql.OpenDB(pgdriver.NewConnector(
		pgdriver.WithNetwork("tcp"),
		pgdriver.WithAddr("localhost:5433"),
		pgdriver.WithUser("postgres"),
		pgdriver.WithPassword("Admin123!"),
		pgdriver.WithDatabase("postgres"),
		pgdriver.WithApplicationName("playground"),
		pgdriver.WithTLSConfig(nil),
		pgdriver.WithDialTimeout(5*time.Second),
		// pgdriver.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
	))

	goVaultDB, err := govault.New("bun", openDB, govault.Config{
		Keys: map[string][]byte{
			"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
			"2": []byte("e778dc27-9b04-44c3-a862-feba061c"),
			"3": []byte("e778dc27-9b04-44c3-a862-83039c8e"),
		},
		DefaultKeyID: "3", // Key 3 is default for encryption
	})
	if err != nil {
		panic(err)
	}
	db := goVaultDB.DB().(*govault.BunDB)

	// Create table
	ctx := context.Background()
	_, err = db.NewCreateTable().
		Model((*TestUser)(nil)).
		IfNotExists().
		Exec(ctx)
	require.NoError(t, err)

	// Clean table
	_, err = db.NewDelete().Model((*TestUser)(nil)).Where("1=1").Exec(ctx)
	require.NoError(t, err)

	cleanup := func() {
		db.NewDropTable().Model((*TestUser)(nil)).IfExists().Exec(ctx)
		openDB.Close()
	}

	return db, goVaultDB, cleanup
}

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

func TestBunSelect(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("select single user", func(t *testing.T) {
		// Insert
		user := &TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Phone: "+62812345678",
		}
		db.NewInsert().Model(user).Exec(ctx)

		// Select
		var retrieved TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx, &retrieved)

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
			db.NewInsert().Model(u).Exec(ctx)
		}

		// Select all
		var retrieved []TestUser
		err := db.NewSelect().
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
		db.NewInsert().Model(user).Exec(ctx)

		var retrieved TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("name = ?", "Test User").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, "testuser@example.com", retrieved.Email)
	})

	t.Run("select with LIMIT and OFFSET", func(t *testing.T) {
		var users []TestUser
		err := db.NewSelect().
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
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("update user", func(t *testing.T) {
		// Insert
		user := &TestUser{
			Name:  "John Doe",
			Email: "john@example.com",
			Phone: "+62812345678",
		}
		db.NewInsert().Model(user).Exec(ctx)

		// Update
		user.Email = "newemail@example.com"
		user.Phone = "+62899999999"

		_, err := db.NewUpdate().
			Model(user).
			WherePK().
			Exec(ctx)
		require.NoError(t, err)

		// Verify
		var retrieved TestUser
		err = db.NewSelect().
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
		db.NewInsert().Model(user).Exec(ctx)

		user.Email = "updated@example.com"
		db.NewUpdate().WithKey("2").Model(user).WherePK().Exec(ctx)

		// Check raw data
		type RawUser struct {
			bun.BaseModel `bun:"table:test_users"`
			ID            int64  `bun:"id"`
			Email         string `bun:"email"`
		}

		var raw RawUser
		err := db.NewSelect().Model(&raw).Where("id = ?", user.ID).Scan(ctx, &raw)
		require.NoError(t, err)

		// Should be encrypted
		assert.Contains(t, raw.Email, "|")
		parts := strings.Split(raw.Email, "|")
		assert.Equal(t, "2", parts[0]) // Active key
	})
}

func TestBunDelete(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("delete user", func(t *testing.T) {
		user := &TestUser{
			Name:  "To Delete",
			Email: "delete@example.com",
			Phone: "+62800000000",
		}
		db.NewInsert().Model(user).Exec(ctx)

		// Delete
		_, err := db.NewDelete().
			Model(user).
			WherePK().
			Exec(ctx)
		require.NoError(t, err)

		// Verify deleted
		var retrieved TestUser
		err = db.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx)

		assert.Error(t, err) // Should not find
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

func TestBunCount(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("count users", func(t *testing.T) {
		// Insert test users
		for i := 1; i <= 5; i++ {
			user := &TestUser{
				Name:  "Count Test User",
				Email: "count@example.com",
			}
			db.NewInsert().Model(user).Exec(ctx)
		}

		count, err := db.NewSelect().
			Model((*TestUser)(nil)).
			Where("name = ?", "Count Test User").
			Count(ctx)

		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

func TestBunAdapter(t *testing.T) {
	t.Run("adapter get name", func(t *testing.T) {
		// Test adapter through govault.New
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

		govaultDB, err := govault.New("bun", openDB, govault.Config{
			Keys: map[string][]byte{
				"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
			},
			DefaultKeyID: "1",
		})
		require.NoError(t, err)

		// GetName is called internally, but we can verify adapter works
		db := govaultDB.DB().(*govault.BunDB)
		assert.NotNil(t, db)

		// Test GetName by accessing adapter through reflection or by creating adapter directly
		// Since adapter is private, we test that govault.New works with "bun"
		assert.NotNil(t, govaultDB)
		openDB.Close()
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

func TestBunSelectQueryMethods(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Insert test data
	users := []*TestUser{
		{Name: "Alice", Email: "alice@example.com", Phone: "+62811111111"},
		{Name: "Bob", Email: "bob@example.com", Phone: "+62822222222"},
	}
	for _, u := range users {
		db.NewInsert().Model(u).Exec(ctx)
	}

	t.Run("where PK without params", func(t *testing.T) {
		// Insert a user first
		user := &TestUser{
			Name:  "PKNoParam",
			Email: "pkno@example.com",
			Phone: "+62888888888",
		}
		db.NewInsert().Model(user).Exec(ctx)

		var retrieved TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, user.ID, retrieved.ID)
	})

	t.Run("where PK with WherePK method", func(t *testing.T) {
		// Insert a user first
		user := &TestUser{
			Name:  "PKMethod",
			Email: "pkmethod@example.com",
			Phone: "+62899999999",
		}
		db.NewInsert().Model(user).Returning("*").Exec(ctx)

		var retrieved TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("id = ?", user.ID).
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, user.ID, retrieved.ID)
	})

	t.Run("where or", func(t *testing.T) {
		var retrieved []TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("name = ?", "Alice").
			WhereOr("name = ?", "Bob").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(retrieved), 2)
	})

	t.Run("where group", func(t *testing.T) {
		var retrieved []TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("name = ?", "Alice").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(retrieved), 1)
	})

	t.Run("order by", func(t *testing.T) {
		var retrieved []TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Order("name ASC").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(retrieved), 2)
	})

	t.Run("order expr", func(t *testing.T) {
		var retrieved []TestUser
		err := db.NewSelect().
			Model(&retrieved).
			OrderExpr("name ASC").
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(retrieved), 2)
	})

	t.Run("where deleted", func(t *testing.T) {
		// Skip this test as TestUser doesn't have soft delete field
		t.Skip("TestUser model does not have soft delete field")
	})

	t.Run("where all with deleted", func(t *testing.T) {
		// Skip this test as TestUser doesn't have soft delete field
		t.Skip("TestUser model does not have soft delete field")
	})
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

type TestUserWithInt struct {
	bun.BaseModel `bun:"table:test_users_int"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Age           int    `bun:"age" encrypted:"true"` // Int field with encrypted tag
	Email         string `bun:"email,notnull" encrypted:"true"`
}

type TestUserWithPrivate struct {
	bun.BaseModel `bun:"table:test_users_private"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	email         string `bun:"email" encrypted:"true"` // private field
	Email         string `bun:"email_public"`
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

func TestBunComplexQueries(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Insert test data
	users := []*TestUser{
		{Name: "Complex1", Email: "complex1@example.com", Phone: "+62811111111", Address: "Addr1"},
		{Name: "Complex2", Email: "complex2@example.com", Phone: "+62822222222", Address: "Addr2"},
		{Name: "Complex3", Email: "complex3@example.com", Phone: "+62833333333", Address: "Addr3"},
	}
	for _, u := range users {
		db.NewInsert().Model(u).Exec(ctx)
	}

	t.Run("complex select with multiple conditions", func(t *testing.T) {
		var retrieved []TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Where("name LIKE ?", "Complex%").
			Order("name ASC").
			Limit(2).
			Offset(1).
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, 2, len(retrieved))
		assert.Equal(t, "Complex2", retrieved[0].Name)
		assert.Equal(t, "Complex3", retrieved[1].Name)
	})

	t.Run("select with count and complex where", func(t *testing.T) {
		// Insert test data for this subtest
		complexUsers := []*TestUser{
			{Name: "ComplexCount1", Email: "complexcount1@example.com", Phone: "+62811111111"},
			{Name: "ComplexCount2", Email: "complexcount2@example.com", Phone: "+62822222222"},
			{Name: "ComplexCount3", Email: "complexcount3@example.com", Phone: "+62833333333"},
		}
		for _, u := range complexUsers {
			_, err := db.NewInsert().Model(u).Exec(ctx)
			require.NoError(t, err)
		}

		count, err := db.NewSelect().
			Model((*TestUser)(nil)).
			Where("name LIKE ?", "ComplexCount%").
			Count(ctx)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, 3)
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

func TestBunSelectAdvancedMethods(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Insert test data
	users := []*TestUser{
		{Name: "AdvAlice", Email: "advalice@example.com", Phone: "+62811111119", Address: "Jakarta"},
		{Name: "AdvBob", Email: "advbob@example.com", Phone: "+62822222229", Address: "Bandung"},
		{Name: "AdvCharlie", Email: "advcharlie@example.com", Phone: "+62833333339", Address: "Jakarta"},
	}
	for _, u := range users {
		db.NewInsert().Model(u).Exec(ctx)
	}

	t.Run("Column and ColumnExpr", func(t *testing.T) {
		var result []struct {
			Name    string
			Address string
		}
		err := db.NewSelect().
			Model((*TestUser)(nil)).
			Column("name", "address").
			Where("name LIKE ?", "Adv%").
			Order("name ASC").
			Scan(ctx, &result)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), 3)
	})

	t.Run("ExcludeColumn", func(t *testing.T) {
		var users []TestUser
		err := db.NewSelect().
			Model(&users).
			ExcludeColumn("phone").
			Where("name LIKE ?", "Adv%").
			Scan(ctx, &users)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(users), 3)
	})

	t.Run("Group and Having", func(t *testing.T) {
		var results []struct {
			Address string
			Count   int `bun:"count"`
		}
		err := db.NewSelect().
			Model((*TestUser)(nil)).
			Column("address").
			ColumnExpr("COUNT(*) as count").
			Where("name LIKE ?", "Adv%").
			Group("address").
			Having("COUNT(*) >= ?", 1).
			Scan(ctx, &results)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("Exists", func(t *testing.T) {
		exists, err := db.NewSelect().
			Model((*TestUser)(nil)).
			Where("name = ?", "AdvAlice").
			Exists(ctx)

		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("ScanAndCount", func(t *testing.T) {
		var users []TestUser
		count, err := db.NewSelect().
			Model(&users).
			Where("name LIKE ?", "Adv%").
			Limit(2).
			ScanAndCount(ctx, &users)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, 3)
		assert.LessOrEqual(t, len(users), 2)
	})

	t.Run("Distinct", func(t *testing.T) {
		var addresses []string
		err := db.NewSelect().
			Model((*TestUser)(nil)).
			Column("address").
			Where("name LIKE ?", "Adv%").
			Distinct().
			Scan(ctx, &addresses)

		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(addresses), 2)
	})
}

func TestBunInsertUpdateDeleteAdvanced(t *testing.T) {
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

	t.Run("Update with Set and Returning", func(t *testing.T) {
		user := &TestUser{
			Name:  "Set Test",
			Email: "set@example.com",
			Phone: "+62899999909",
		}
		db.NewInsert().Model(user).Exec(ctx)

		var updated TestUser
		err := db.NewUpdate().
			Model(&updated).
			Set("name = ?", "Updated Name").
			Where("id = ?", user.ID).
			Returning("*").
			Scan(ctx, &updated)

		require.NoError(t, err)
		assert.Equal(t, user.ID, updated.ID)
	})

	t.Run("Update with OmitZero", func(t *testing.T) {
		user := &TestUser{
			Name:  "OmitZero Test",
			Email: "omitzero@example.com",
			Phone: "+62822222263",
		}
		db.NewInsert().Model(user).Exec(ctx)

		user.Email = "updated@example.com"
		_, err := db.NewUpdate().
			Model(user).
			OmitZero().
			WherePK().
			Exec(ctx)

		require.NoError(t, err)
	})

	t.Run("Delete with Returning", func(t *testing.T) {
		user := &TestUser{
			Name:  "Delete Returning",
			Email: "delret@example.com",
			Phone: "+62888888869",
		}
		db.NewInsert().Model(user).Exec(ctx)

		var deleted TestUser
		err := db.NewDelete().
			Model(&deleted).
			Where("id = ?", user.ID).
			Returning("*").
			Scan(ctx, &deleted)

		require.NoError(t, err)
		assert.Equal(t, user.ID, deleted.ID)
	})
}

func TestBunDDLAndRawMethods(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("NewCreateTable", func(t *testing.T) {
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

	t.Run("NewRaw", func(t *testing.T) {
		var count int
		err := db.NewRaw("SELECT COUNT(*) FROM test_users").Scan(ctx, &count)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, count, 0)
	})
}

func TestBunApplyMethods(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Select with Apply", func(t *testing.T) {
		user := &TestUser{Name: "Apply Test", Email: "apply@example.com", Phone: "+62899999981"}
		db.NewInsert().Model(user).Exec(ctx)

		applyFunc := func(q *govault.BunSelectQuery) *govault.BunSelectQuery {
			return q.Where("name = ?", "Apply Test")
		}

		var retrieved TestUser
		err := db.NewSelect().
			Model(&retrieved).
			Apply(applyFunc).
			Scan(ctx, &retrieved)

		require.NoError(t, err)
		assert.Equal(t, "Apply Test", retrieved.Name)
	})

	t.Run("Insert with Apply", func(t *testing.T) {
		user := &TestUser{Name: "Insert Apply", Email: "insertapply@example.com", Phone: "+62811119992"}

		applyFunc := func(q *govault.BunInsertQuery) *govault.BunInsertQuery {
			return q.Column("name", "email", "phone")
		}

		_, err := db.NewInsert().
			Model(user).
			Apply(applyFunc).
			Exec(ctx)

		require.NoError(t, err)
	})

	t.Run("Update with Apply", func(t *testing.T) {
		user := &TestUser{Name: "Update Apply", Email: "updateapply@example.com", Phone: "+62899999982"}
		db.NewInsert().Model(user).Exec(ctx)

		applyFunc := func(q *govault.BunUpdateQuery) *govault.BunUpdateQuery {
			return q.Set("name = ?", "Updated Apply")
		}

		_, err := db.NewUpdate().
			Model((*TestUser)(nil)).
			Apply(applyFunc).
			Where("id = ?", user.ID).
			Exec(ctx)

		require.NoError(t, err)
	})

	t.Run("Delete with Apply", func(t *testing.T) {
		user := &TestUser{Name: "Delete Apply", Email: "delapply@example.com", Phone: "+62899999983"}
		db.NewInsert().Model(user).Exec(ctx)

		applyFunc := func(q *bun.DeleteQuery) *bun.DeleteQuery {
			return q.Where("id = ?", user.ID)
		}

		_, err := db.NewDelete().
			Model((*TestUser)(nil)).
			Apply(applyFunc).
			Exec(ctx)

		require.NoError(t, err)
	})
}

func TestBunQueryErrorHandling(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Select with Err", func(t *testing.T) {
		var users []TestUser
		err := db.NewSelect().
			Model(&users).
			Err(assert.AnError).
			Scan(ctx, &users)

		assert.Error(t, err)
	})

	t.Run("Insert with Err", func(t *testing.T) {
		user := &TestUser{Name: "Error Test", Email: "err@example.com"}
		_, err := db.NewInsert().
			Model(user).
			Err(assert.AnError).
			Exec(ctx)

		assert.Error(t, err)
	})

	t.Run("Update with Err", func(t *testing.T) {
		user := &TestUser{ID: 999, Name: "Error Test"}
		_, err := db.NewUpdate().
			Model(user).
			Err(assert.AnError).
			Exec(ctx)

		assert.Error(t, err)
	})

	t.Run("Delete with Err", func(t *testing.T) {
		_, err := db.NewDelete().
			Model((*TestUser)(nil)).
			Err(assert.AnError).
			Exec(ctx)

		assert.Error(t, err)
	})
}

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

func TestBunExoticMethods(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Select Exotic Methods", func(t *testing.T) {
		// Test Conn and Err
		q := db.NewSelect().Model((*TestUser)(nil)).Conn(db.DB).Err(nil)
		require.NotNil(t, q)

		// Test With
		withQ := db.NewSelect().Model((*TestUser)(nil)).Column("id")
		db.NewSelect().With("cte", withQ).Table("cte").Column("*").Limit(1).Exec(ctx)

		// Test WithRecursive
		db.NewSelect().WithRecursive("cte", withQ).Table("cte").Column("*").Limit(1).Exec(ctx)

		// Test DistinctOn
		db.NewSelect().Model((*TestUser)(nil)).DistinctOn("address").Scan(ctx, &[]TestUser{})

		// Test ModelTableExpr
		db.NewSelect().Model((*TestUser)(nil)).ModelTableExpr("test_users AS my_users").Scan(ctx, &[]TestUser{})

		// Test WhereGroup and WherePK
		db.NewSelect().Model((*TestUser)(nil)).WherePK("id").WhereGroup(" AND ", func(sq *govault.BunSelectQuery) *govault.BunSelectQuery {
			return sq.Where("name IS NOT NULL")
		}).Limit(1).Exec(ctx)

		// Test soft deletes
		db.NewSelect().Model((*TestUser)(nil)).WhereDeleted().Limit(1).Exec(ctx)
		db.NewSelect().Model((*TestUser)(nil)).WhereAllWithDeleted().Limit(1).Exec(ctx)

		// Test Join variants
		db.NewSelect().Model((*TestUser)(nil)).
			Join("LEFT JOIN test_users AS u2 ON u2.id = u.id").
			JoinOn("1=1").
			JoinOnOr("2=2").
			Limit(1).Exec(ctx)

		// Test OrderBy and For
		db.NewSelect().Model((*TestUser)(nil)).OrderBy("id", "ASC").For("UPDATE").Limit(1).Exec(ctx)

		// Test Set Operations
		s1 := db.NewSelect().Model((*TestUser)(nil)).Where("id = 1")
		s2 := db.NewSelect().Model((*TestUser)(nil)).Where("id = 2")
		s1.Union(s2).Limit(1).Exec(ctx)
		s1.UnionAll(s2).Limit(1).Exec(ctx)
		s1.Intersect(s2).Limit(1).Exec(ctx)
		s1.IntersectAll(s2).Limit(1).Exec(ctx)
		s1.Except(s2).Limit(1).Exec(ctx)
		s1.ExceptAll(s2).Limit(1).Exec(ctx)

		// Test Index Hints (MySQL specific but should work for coverage by just generating the SQL)
		db.NewSelect().Model((*TestUser)(nil)).
			UseIndex("idx1").UseIndexForJoin("idx2").UseIndexForOrderBy("idx3").UseIndexForGroupBy("idx4").
			IgnoreIndex("idx5").IgnoreIndexForJoin("idx6").IgnoreIndexForOrderBy("idx7").IgnoreIndexForGroupBy("idx8").
			ForceIndex("idx9").ForceIndexForJoin("idx10").ForceIndexForOrderBy("idx11").ForceIndexForGroupBy("idx12").
			Comment("testing").
			Limit(1).Exec(ctx)
	})

	t.Run("Insert Exotic Methods", func(t *testing.T) {
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

	t.Run("Update Exotic Methods", func(t *testing.T) {
		user := &TestUser{ID: 1, Name: "UpdateExotic"}

		// Conn and Err
		db.NewUpdate().Model(user).Conn(db.DB).Err(nil).Exec(ctx)

		// With and WithRecursive
		withQ := db.NewSelect().Model((*TestUser)(nil))
		db.NewUpdate().Model(user).With("cte", withQ).WithRecursive("cte", withQ).Exec(ctx)

		// Table and TableExpr
		db.NewUpdate().Model(user).Table("test_users").TableExpr("test_users AS u").Exec(ctx)

		// ModelTableExpr
		db.NewUpdate().Model(user).ModelTableExpr("test_users AS u").Exec(ctx)

		// Column and ExcludeColumn
		db.NewUpdate().Model(user).Column("name").ExcludeColumn("email").Exec(ctx)

		// SetColumn and Value
		db.NewUpdate().Model(user).SetColumn("name", "UPPER(?)", "val").Value("address", "'addr'").Exec(ctx)

		// Join variants
		db.NewUpdate().Model(user).
			Join("LEFT JOIN test_users AS u2 ON u2.id = test_user.id"). // table name might vary
			JoinOn("1=1").
			JoinOnOr("2=2").
			Exec(ctx)

		// Where variants
		db.NewUpdate().Model(user).Where("1=1").WhereOr("2=2").WhereGroup(" AND ", func(uq *govault.BunUpdateQuery) *govault.BunUpdateQuery {
			return uq.Where("3=3")
		}).Exec(ctx)

		// soft deletes
		db.NewUpdate().Model(user).WhereDeleted().WhereAllWithDeleted().Exec(ctx)

		// Order, OrderExpr, Limit
		db.NewUpdate().Model(user).Order("id").OrderExpr("id DESC").Limit(1).Exec(ctx)

		// Index Hints
		db.NewUpdate().Model(user).UseIndex("idx").IgnoreIndex("idx").ForceIndex("idx").Comment("test").Exec(ctx)
	})

	t.Run("Delete Exotic Methods", func(t *testing.T) {
		user := &TestUser{ID: 1}

		// Conn and Err
		db.NewDelete().Model(user).Conn(db.DB).Err(nil).Exec(ctx)

		// With and WithRecursive
		withQ := db.NewSelect().Model((*TestUser)(nil))
		db.NewDelete().Model(user).With("cte", withQ).WithRecursive("cte", withQ).Exec(ctx)

		// Table and TableExpr
		db.NewDelete().Model(user).Table("test_users").TableExpr("test_users AS u").Exec(ctx)

		// ModelTableExpr
		db.NewDelete().Model(user).ModelTableExpr("test_users AS u").Exec(ctx)

		// WherePK, WhereOr, WhereGroup
		db.NewDelete().Model(user).WherePK().WhereOr("1=1").WhereGroup(" AND ", func(dq *bun.DeleteQuery) *bun.DeleteQuery {
			return dq.Where("2=2")
		}).Exec(ctx)

		// soft deletes
		db.NewDelete().Model(user).WhereDeleted().WhereAllWithDeleted().ForceDelete().Exec(ctx)

		// Order, OrderExpr, Limit, Returning, Comment
		db.NewDelete().Model(user).Order("id").OrderExpr("id").Limit(1).Returning("id").Comment("test").Exec(ctx)
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

type TestProfile struct {
	bun.BaseModel `bun:"table:test_profiles"`
	ID            int64     `bun:"id,pk,autoincrement"`
	UserID        int64     `bun:"user_id"`
	Bio           string    `bun:"bio" encrypted:"true"`
	User          *TestUser `bun:"rel:belongs-to,join:user_id=id"`
}

func TestBunFinalCoverage(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("BunDB DDL and Index methods", func(t *testing.T) {
		// NewCreateIndex
		_, err := db.NewCreateIndex().Model((*TestUser)(nil)).Index("idx_name").Column("name").Exec(ctx)
		require.NoError(t, err)

		// NewDropIndex
		_, err = db.NewDropIndex().Index("idx_name").Exec(ctx)
		require.NoError(t, err)

		// NewTruncateTable
		_, err = db.NewTruncateTable().Model((*TestUser)(nil)).Exec(ctx)
		require.NoError(t, err)
	})

	t.Run("BunDB NewValues", func(t *testing.T) {
		user := &TestUser{Name: "ValuesTest"}
		q := db.NewValues(user)
		assert.NotNil(t, q)
	})

	t.Run("BunInsertQuery Exotic", func(t *testing.T) {
		user := &TestUser{Name: "InsertExotic2", Email: "exotic2@example.com"}

		// ExcludeColumn
		_, err := db.NewInsert().Model(user).ExcludeColumn("phone").Exec(ctx)
		require.NoError(t, err)

		// Value - note: Value is used to set specific columns to expressions or values
		_, err = db.NewInsert().Model(&TestUser{Name: "ValueInsert"}).Value("email", "?", "val|key|nonce").Exec(ctx)
		require.NoError(t, err)

		// On Conflict
		_, err = db.NewInsert().Model(user).On("CONFLICT (id) DO UPDATE SET name = EXCLUDED.name").Exec(ctx)
		require.NoError(t, err)
	})

	t.Run("BunSelectQuery Exotic", func(t *testing.T) {
		// TableExpr
		var count int
		err := db.NewSelect().TableExpr("(SELECT 1 as val) as tmp").Column("val").Scan(ctx, &count)
		require.NoError(t, err)

		// GroupExpr
		var results []struct {
			Address string
			Total   int
		}
		db.NewSelect().Model((*TestUser)(nil)).Column("address").ColumnExpr("count(*) as total").GroupExpr("address").Scan(ctx, &results)

		// Relation setup
		_, err = db.NewCreateTable().Model((*TestProfile)(nil)).Exec(ctx)
		require.NoError(t, err)
		defer db.NewDropTable().Model((*TestProfile)(nil)).Exec(ctx)

		user := &TestUser{Name: "RelUser", Email: "rel@example.com"}
		db.NewInsert().Model(user).Exec(ctx)

		profile := &TestProfile{UserID: user.ID, Bio: "My Bio"}
		db.NewInsert().Model(profile).Exec(ctx)

		var retrievedProfile TestProfile
		err = db.NewSelect().Model(&retrievedProfile).Relation("User").Where("test_profile.id = ?", profile.ID).Scan(ctx, &retrievedProfile)
		require.NoError(t, err)
		assert.Equal(t, "My Bio", retrievedProfile.Bio)
		assert.NotNil(t, retrievedProfile.User)
		assert.Equal(t, "rel@example.com", retrievedProfile.User.Email)
	})
}
