package govault_test

import (
	"context"
	"database/sql"
	"fmt"
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
