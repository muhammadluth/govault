// Package govault - Bun adapter delete query tests
package bun_test

import (
	"context"
	"testing"

	gb "github.com/muhammadluth/govault/bun"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestBunDeleteAdvanced(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

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

func TestBunDeleteApply(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	user := &TestUser{Name: "Delete Apply", Email: "delapply@example.com", Phone: "+62899999983"}
	db.NewInsert().Model(user).Exec(ctx)

	applyFunc := func(q *gb.BunDeleteQuery) *gb.BunDeleteQuery {
		return q.Where("id = ?", user.ID)
	}

	_, err := db.NewDelete().
		Model((*TestUser)(nil)).
		Apply(applyFunc).
		Exec(ctx)

	require.NoError(t, err)
}

func TestBunDeleteExotic(t *testing.T) {
	db, _, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

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
		db.NewDelete().Model(user).WherePK().WhereOr("1=1").WhereGroup(" AND ", func(dq *gb.BunDeleteQuery) *gb.BunDeleteQuery {
			return dq.Where("2=2")
		}).Exec(ctx)

		// soft deletes
		db.NewDelete().Model(user).WhereDeleted().WhereAllWithDeleted().ForceDelete().Exec(ctx)

		// Order, OrderExpr, Limit, Returning, Comment
		db.NewDelete().Model(user).Order("id").OrderExpr("id").Limit(1).Returning("id").Comment("test").Exec(ctx)
	})
}
