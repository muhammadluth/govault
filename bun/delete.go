// Package govault - Bun adapter delete query implementation
package bun

import (
	"context"
	"database/sql"

	"github.com/muhammadluth/govault/internal"
	"github.com/uptrace/bun"
)

// BunDeleteQuery wraps bun.DeleteQuery
type BunDeleteQuery struct {
	*bun.DeleteQuery
	govault *internal.GovaultDB
	keyID   string
}

// Conn sets the database connection
func (q *BunDeleteQuery) Conn(db bun.IConn) *BunDeleteQuery {
	q.DeleteQuery.Conn(db)
	return q
}

// Model sets the model and encrypts fields
func (q *BunDeleteQuery) Model(model any) *BunDeleteQuery {
	q.DeleteQuery.Model(model)
	return q
}

// Err sets an error on the query
func (q *BunDeleteQuery) Err(err error) *BunDeleteQuery {
	q.DeleteQuery.Err(err)
	return q
}

// Apply applies functions to the query
func (q *BunDeleteQuery) Apply(fns ...func(*BunDeleteQuery) *BunDeleteQuery) *BunDeleteQuery {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// With adds a WITH clause
func (q *BunDeleteQuery) With(name string, query bun.Query) *BunDeleteQuery {
	q.DeleteQuery.With(name, query)
	return q
}

// Table specifies the table to delete from
func (q *BunDeleteQuery) Table(tables ...string) *BunDeleteQuery {
	q.DeleteQuery.Table(tables...)
	return q
}

// Where adds a WHERE clause
func (q *BunDeleteQuery) Where(query string, args ...any) *BunDeleteQuery {
	q.DeleteQuery.Where(query, args...)
	return q
}

// WhereOr adds a WHERE clause with OR
func (q *BunDeleteQuery) WhereOr(query string, args ...any) *BunDeleteQuery {
	q.DeleteQuery.WhereOr(query, args...)
	return q
}

// WhereGroup groups WHERE conditions
func (q *BunDeleteQuery) WhereGroup(sep string, fn func(*BunDeleteQuery) *BunDeleteQuery) *BunDeleteQuery {
	q.DeleteQuery.WhereGroup(sep, func(dq *bun.DeleteQuery) *bun.DeleteQuery {
		return fn(q).DeleteQuery
	})
	return q
}

// WherePK adds a WHERE condition on primary key
func (q *BunDeleteQuery) WherePK(cols ...string) *BunDeleteQuery {
	q.DeleteQuery.WherePK(cols...)
	return q
}

// WhereDeleted adds a WHERE condition for soft-deleted rows
func (q *BunDeleteQuery) WhereDeleted() *BunDeleteQuery {
	q.DeleteQuery.WhereDeleted()
	return q
}

// WhereAllWithDeleted includes both active and soft-deleted rows
func (q *BunDeleteQuery) WhereAllWithDeleted() *BunDeleteQuery {
	q.DeleteQuery.WhereAllWithDeleted()
	return q
}

// ForceDelete forces deletion of soft-deleted rows
func (q *BunDeleteQuery) ForceDelete() *BunDeleteQuery {
	q.DeleteQuery.ForceDelete()
	return q
}

// Returning adds a RETURNING clause
func (q *BunDeleteQuery) Returning(query string, args ...any) *BunDeleteQuery {
	q.DeleteQuery.Returning(query, args...)
	return q
}

// Exec executes the delete query
func (q *BunDeleteQuery) Exec(ctx context.Context, dest ...any) (sql.Result, error) {
	res, err := q.DeleteQuery.Exec(ctx, dest...)
	if err != nil {
		return res, err
	}
	if len(dest) > 0 {
		for _, d := range dest {
			if err := q.govault.DecryptRecursive(d); err != nil {
				return res, err
			}
		}
	}
	return res, nil
}

// WithKey sets the encryption key for this query
func (q *BunDeleteQuery) WithKey(keyID string) *BunDeleteQuery {
	q.keyID = keyID
	return q
}
