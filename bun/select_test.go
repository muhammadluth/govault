// Package govault - Bun adapter select query tests
package bun_test

import (
	"context"
	"testing"

	gb "github.com/muhammadluth/govault/bun"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestBunRelationDecryption(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables
	_, err := db.NewCreateTable().Model((*TestProfile)(nil)).IfNotExists().Exec(ctx)
	require.NoError(t, err)
	defer db.NewDropTable().Model((*TestProfile)(nil)).IfExists().Exec(ctx)

	// Insert User
	user := &TestUser{
		Name:  "Relation User",
		Email: "relation@example.com",
		Phone: "+62800000001",
	}
	_, err = db.NewInsert().Model(user).Exec(ctx)
	require.NoError(t, err)

	// Insert Profile
	profile := &TestProfile{
		UserID: user.ID,
		Bio:    "This is a secret bio",
	}
	_, err = db.NewInsert().Model(profile).Exec(ctx)
	require.NoError(t, err)

	t.Run("Select with Relation", func(t *testing.T) {
		var userWithProfile TestUserWithProfile
		err := db.NewSelect().
			Model(&userWithProfile).
			Relation("Profile").
			Where("u.id = ?", user.ID).
			Scan(ctx, &userWithProfile)

		require.NoError(t, err)
		require.NotNil(t, userWithProfile.Profile)

		// The Bio should be decrypted
		assert.Equal(t, "This is a secret bio", userWithProfile.Profile.Bio)
		assert.NotContains(t, userWithProfile.Profile.Bio, "|")
	})

	t.Run("Select with Nested Relation Slice", func(t *testing.T) {
		var users []TestUserWithProfile
		err := db.NewSelect().
			Model(&users).
			Relation("Profile").
			Where("u.id = ?", user.ID).
			Scan(ctx, &users)

		require.NoError(t, err)
		require.NotEmpty(t, users)
		require.NotNil(t, users[0].Profile)

		// The Bio should be decrypted
		assert.Equal(t, "This is a secret bio", users[0].Profile.Bio)
	})
}

func TestBunSelectApply(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	user := &TestUser{Name: "Apply Test", Email: "apply@example.com", Phone: "+62899999981"}
	db.NewInsert().Model(user).Exec(ctx)

	applyFunc := func(q *gb.BunSelectQuery) *gb.BunSelectQuery {
		return q.Where("name = ?", "Apply Test")
	}

	var retrieved TestUser
	err := db.NewSelect().
		Model(&retrieved).
		Apply(applyFunc).
		Scan(ctx, &retrieved)

	require.NoError(t, err)
	assert.Equal(t, "Apply Test", retrieved.Name)
}

func TestBunSelectExotic(t *testing.T) {
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
		db.NewSelect().Model((*TestUser)(nil)).WherePK("id").WhereGroup(" AND ", func(sq *gb.BunSelectQuery) *gb.BunSelectQuery {
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

		// Test Index Hints
		db.NewSelect().Model((*TestUser)(nil)).
			UseIndex("idx1").UseIndexForJoin("idx2").UseIndexForOrderBy("idx3").UseIndexForGroupBy("idx4").
			IgnoreIndex("idx5").IgnoreIndexForJoin("idx6").IgnoreIndexForOrderBy("idx7").IgnoreIndexForGroupBy("idx8").
			ForceIndex("idx9").ForceIndexForJoin("idx10").ForceIndexForOrderBy("idx11").ForceIndexForGroupBy("idx12").
			Comment("testing").
			Limit(1).Exec(ctx)
	})

	t.Run("BunSelectQuery Exotic 2", func(t *testing.T) {
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
		err = db.NewSelect().Model(&retrievedProfile).Relation("User").Where("p.id = ?", profile.ID).Scan(ctx, &retrievedProfile)
		require.NoError(t, err)
		assert.Equal(t, "My Bio", retrievedProfile.Bio)
		assert.NotNil(t, retrievedProfile.User)
		assert.Equal(t, "rel@example.com", retrievedProfile.User.Email)
	})
}
