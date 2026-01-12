// Package govault - Bun adapter update query implementation
package bun

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/muhammadluth/govault/internal"
	"github.com/uptrace/bun"
)

// BunUpdateQuery wraps bun.UpdateQuery
type BunUpdateQuery struct {
	*bun.UpdateQuery
	govault *internal.GovaultDB
	keyID   string
}

// Conn sets the database connection
func (q *BunUpdateQuery) Conn(db bun.IConn) *BunUpdateQuery {
	q.UpdateQuery.Conn(db)
	return q
}

// Model sets the model and encrypts fields
func (q *BunUpdateQuery) Model(model any) *BunUpdateQuery {
	if err := q.encryptModel(model); err != nil {
		return q.Err(err)
	}
	q.UpdateQuery.Model(model)
	return q
}

// Err sets an error on the query
func (q *BunUpdateQuery) Err(err error) *BunUpdateQuery {
	q.UpdateQuery.Err(err)
	return q
}

// Apply applies functions to the query
func (q *BunUpdateQuery) Apply(fns ...func(*BunUpdateQuery) *BunUpdateQuery) *BunUpdateQuery {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// With adds a WITH clause
func (q *BunUpdateQuery) With(name string, query bun.Query) *BunUpdateQuery {
	q.UpdateQuery.With(name, query)
	return q
}

// WithRecursive adds a WITH RECURSIVE clause
func (q *BunUpdateQuery) WithRecursive(name string, query bun.Query) *BunUpdateQuery {
	q.UpdateQuery.WithRecursive(name, query)
	return q
}

// Table specifies the table to update
func (q *BunUpdateQuery) Table(tables ...string) *BunUpdateQuery {
	q.UpdateQuery.Table(tables...)
	return q
}

// TableExpr adds a table expression
func (q *BunUpdateQuery) TableExpr(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.TableExpr(query, args...)
	return q
}

// ModelTableExpr overrides the table name from model
func (q *BunUpdateQuery) ModelTableExpr(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.ModelTableExpr(query, args...)
	return q
}

// Column adds columns to update
func (q *BunUpdateQuery) Column(columns ...string) *BunUpdateQuery {
	q.UpdateQuery.Column(columns...)
	return q
}

// ExcludeColumn excludes columns from update
func (q *BunUpdateQuery) ExcludeColumn(columns ...string) *BunUpdateQuery {
	q.UpdateQuery.ExcludeColumn(columns...)
	return q
}

// Set adds a SET clause
func (q *BunUpdateQuery) Set(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.Set(query, args...)
	return q
}

// SetColumn sets a specific column
func (q *BunUpdateQuery) SetColumn(column string, query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.SetColumn(column, query, args...)
	return q
}

// Value sets a column value
func (q *BunUpdateQuery) Value(column string, query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.Value(column, query, args...)
	return q
}

// OmitZero omits zero values from update
func (q *BunUpdateQuery) OmitZero() *BunUpdateQuery {
	q.UpdateQuery.OmitZero()
	return q
}

// Join adds a JOIN clause
func (q *BunUpdateQuery) Join(join string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.Join(join, args...)
	return q
}

// JoinOn adds an ON condition to the most recent JOIN
func (q *BunUpdateQuery) JoinOn(cond string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.JoinOn(cond, args...)
	return q
}

// JoinOnOr adds an ON condition with OR
func (q *BunUpdateQuery) JoinOnOr(cond string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.JoinOnOr(cond, args...)
	return q
}

// WherePK adds a WHERE condition on primary key
func (q *BunUpdateQuery) WherePK(cols ...string) *BunUpdateQuery {
	q.UpdateQuery.WherePK(cols...)
	return q
}

// Where adds a WHERE clause
func (q *BunUpdateQuery) Where(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.Where(query, args...)
	return q
}

// WhereOr adds a WHERE clause with OR
func (q *BunUpdateQuery) WhereOr(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.WhereOr(query, args...)
	return q
}

// WhereGroup groups WHERE conditions
func (q *BunUpdateQuery) WhereGroup(sep string, fn func(*BunUpdateQuery) *BunUpdateQuery) *BunUpdateQuery {
	q.UpdateQuery.WhereGroup(sep, func(uq *bun.UpdateQuery) *bun.UpdateQuery {
		return fn(q).UpdateQuery
	})
	return q
}

// WhereDeleted adds a WHERE condition for soft-deleted rows
func (q *BunUpdateQuery) WhereDeleted() *BunUpdateQuery {
	q.UpdateQuery.WhereDeleted()
	return q
}

// WhereAllWithDeleted includes both active and soft-deleted rows
func (q *BunUpdateQuery) WhereAllWithDeleted() *BunUpdateQuery {
	q.UpdateQuery.WhereAllWithDeleted()
	return q
}

// Order adds an ORDER BY clause
func (q *BunUpdateQuery) Order(orders ...string) *BunUpdateQuery {
	q.UpdateQuery.Order(orders...)
	return q
}

// OrderExpr adds an ORDER BY expression
func (q *BunUpdateQuery) OrderExpr(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.OrderExpr(query, args...)
	return q
}

// Limit sets the maximum number of rows to update
func (q *BunUpdateQuery) Limit(n int) *BunUpdateQuery {
	q.UpdateQuery.Limit(n)
	return q
}

// Returning adds a RETURNING clause
func (q *BunUpdateQuery) Returning(query string, args ...any) *BunUpdateQuery {
	q.UpdateQuery.Returning(query, args...)
	return q
}

// UseIndex adds a USE INDEX hint (MySQL)
func (q *BunUpdateQuery) UseIndex(indexes ...string) *BunUpdateQuery {
	q.UpdateQuery.UseIndex(indexes...)
	return q
}

// IgnoreIndex adds an IGNORE INDEX hint (MySQL)
func (q *BunUpdateQuery) IgnoreIndex(indexes ...string) *BunUpdateQuery {
	q.UpdateQuery.IgnoreIndex(indexes...)
	return q
}

// ForceIndex adds a FORCE INDEX hint (MySQL)
func (q *BunUpdateQuery) ForceIndex(indexes ...string) *BunUpdateQuery {
	q.UpdateQuery.ForceIndex(indexes...)
	return q
}

// Comment adds a comment to the query
func (q *BunUpdateQuery) Comment(comment string) *BunUpdateQuery {
	q.UpdateQuery.Comment(comment)
	return q
}

// Exec executes the update query
func (q *BunUpdateQuery) Exec(ctx context.Context, dest ...any) (sql.Result, error) {
	res, err := q.UpdateQuery.Exec(ctx, dest...)
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

// Scan executes the query and scans the result
func (q *BunUpdateQuery) Scan(ctx context.Context, dest ...any) error {
	err := q.UpdateQuery.Scan(ctx, dest...)
	if err != nil {
		return err
	}
	for _, d := range dest {
		if err := q.govault.DecryptRecursive(d); err != nil {
			return err
		}
	}
	return nil
}

// WithKey sets the encryption key for this query
func (q *BunUpdateQuery) WithKey(keyID string) *BunUpdateQuery {
	q.keyID = keyID
	return q
}

// encryptModel encrypts fields tagged with encrypted:"true"
func (q *BunUpdateQuery) encryptModel(model any) error {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		if !field.CanSet() {
			continue
		}

		if fieldType.Tag.Get("encrypted") == "true" {
			if field.Kind() == reflect.String {
				plaintext := field.String()
				if plaintext != "" {
					var encrypted string
					var err error

					if q.keyID != "" {
						encrypted, err = q.govault.Encrypt(plaintext, q.keyID)
					} else {
						encrypted, err = q.govault.Encrypt(plaintext)
					}

					if err != nil {
						return fmt.Errorf("failed to encrypt field %s: %w", fieldType.Name, err)
					}
					field.SetString(encrypted)
				}
			}
		}
	}

	return nil
}
