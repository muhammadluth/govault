// Package govault - Bun adapter update query tests
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

func TestBunUpdateAdvanced(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

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
}

func TestBunUpdateApply(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	user := &TestUser{Name: "Update Apply", Email: "updateapply@example.com", Phone: "+62899999982"}
	db.NewInsert().Model(user).Exec(ctx)

	applyFunc := func(q *gb.BunUpdateQuery) *gb.BunUpdateQuery {
		return q.Set("name = ?", "Updated Apply")
	}

	_, err := db.NewUpdate().
		Model((*TestUser)(nil)).
		Apply(applyFunc).
		Where("id = ?", user.ID).
		Exec(ctx)

	require.NoError(t, err)
}

func TestBunUpdateExotic(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

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
			Join("LEFT JOIN test_users AS u2 ON u2.id = test_users.id").
			JoinOn("1=1").
			JoinOnOr("2=2").
			Exec(ctx)

		// Where variants
		db.NewUpdate().Model(user).Where("1=1").WhereOr("2=2").WhereGroup(" AND ", func(uq *gb.BunUpdateQuery) *gb.BunUpdateQuery {
			return uq.Where("3=3")
		}).Exec(ctx)

		// soft deletes
		db.NewUpdate().Model(user).WhereDeleted().WhereAllWithDeleted().Exec(ctx)

		// Order, OrderExpr, Limit
		db.NewUpdate().Model(user).Order("id").OrderExpr("id DESC").Limit(1).Exec(ctx)

		// Index Hints
		db.NewUpdate().Model(user).UseIndex("idx").IgnoreIndex("idx").ForceIndex("idx").Comment("test").Exec(ctx)
	})
}
