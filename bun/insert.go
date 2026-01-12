// Package govault - Bun adapter insert query implementation
package bun

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/muhammadluth/govault/internal"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
)

// BunInsertQuery wraps bun.InsertQuery
type BunInsertQuery struct {
	*bun.InsertQuery
	govault *internal.GovaultDB
	keyID   string
}

// Conn sets the database connection
func (q *BunInsertQuery) Conn(db bun.IConn) *BunInsertQuery {
	q.InsertQuery.Conn(db)
	return q
}

// Model sets the model and encrypts fields
func (q *BunInsertQuery) Model(model any) *BunInsertQuery {
	if err := q.encryptModel(model); err != nil {
		return q.Err(err)
	}
	q.InsertQuery.Model(model)
	return q
}

// Err sets an error on the query
func (q *BunInsertQuery) Err(err error) *BunInsertQuery {
	q.InsertQuery.Err(err)
	return q
}

// Apply applies functions to the query
func (q *BunInsertQuery) Apply(fns ...func(*BunInsertQuery) *BunInsertQuery) *BunInsertQuery {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// With adds a WITH clause
func (q *BunInsertQuery) With(name string, query bun.Query) *BunInsertQuery {
	q.InsertQuery.With(name, query)
	return q
}

// WithRecursive adds a WITH RECURSIVE clause
func (q *BunInsertQuery) WithRecursive(name string, query bun.Query) *BunInsertQuery {
	q.InsertQuery.WithRecursive(name, query)
	return q
}

// WithQuery adds a WITH QUERY clause
func (q *BunInsertQuery) WithQuery(query *bun.WithQuery) *BunInsertQuery {
	q.InsertQuery.WithQuery(query)
	return q
}

// Table specifies the table to insert into
func (q *BunInsertQuery) Table(tables ...string) *BunInsertQuery {
	q.InsertQuery.Table(tables...)
	return q
}

// TableExpr adds a table expression
func (q *BunInsertQuery) TableExpr(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.TableExpr(query, args...)
	return q
}

// ModelTableExpr overrides the table name from model
func (q *BunInsertQuery) ModelTableExpr(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.ModelTableExpr(query, args...)
	return q
}

// Column adds columns to insert
func (q *BunInsertQuery) Column(columns ...string) *BunInsertQuery {
	q.InsertQuery.Column(columns...)
	return q
}

// ColumnExpr adds a column expression
func (q *BunInsertQuery) ColumnExpr(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.ColumnExpr(query, args...)
	return q
}

// ExcludeColumn excludes columns from insert
func (q *BunInsertQuery) ExcludeColumn(columns ...string) *BunInsertQuery {
	q.InsertQuery.ExcludeColumn(columns...)
	return q
}

// Value sets a column value
func (q *BunInsertQuery) Value(column string, expr string, args ...any) *BunInsertQuery {
	q.InsertQuery.Value(column, expr, args...)
	return q
}

// Where adds a WHERE clause
func (q *BunInsertQuery) Where(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.Where(query, args...)
	return q
}

// WhereOr adds a WHERE clause with OR
func (q *BunInsertQuery) WhereOr(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.WhereOr(query, args...)
	return q
}

// Returning adds a RETURNING clause
func (q *BunInsertQuery) Returning(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.Returning(query, args...)
	return q
}

// Ignore generates INSERT IGNORE (MySQL) or ON CONFLICT DO NOTHING (PostgreSQL)
func (q *BunInsertQuery) Ignore() *BunInsertQuery {
	q.InsertQuery.Ignore()
	return q
}

// Replace generates REPLACE INTO (MySQL)
func (q *BunInsertQuery) Replace() *BunInsertQuery {
	q.InsertQuery.Replace()
	return q
}

// Comment adds a comment to the query
func (q *BunInsertQuery) Comment(comment string) *BunInsertQuery {
	q.InsertQuery.Comment(comment)
	return q
}

func (q *BunInsertQuery) Operation() string {
	return q.InsertQuery.Operation()
}

func (q *BunInsertQuery) AppendQuery(gen schema.QueryGen, b []byte) ([]byte, error) {
	return q.InsertQuery.AppendQuery(gen, b)
}

// On adds an ON CONFLICT clause (PostgreSQL)
func (q *BunInsertQuery) On(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.On(query, args...)
	return q
}

func (q *BunInsertQuery) Set(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.Set(query, args...)
	return q
}

func (q *BunInsertQuery) SetValues(values bun.ValuesQuery) *BunInsertQuery {
	q.InsertQuery.SetValues(&values)
	return q
}

// Scan executes the query and scans the result
func (q *BunInsertQuery) Scan(ctx context.Context, dest ...any) error {
	err := q.InsertQuery.Scan(ctx, dest...)
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

// Exec executes the insert query
func (q *BunInsertQuery) Exec(ctx context.Context, dest ...any) (sql.Result, error) {
	res, err := q.InsertQuery.Exec(ctx, dest...)
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

func (q *BunInsertQuery) String() string {
	return q.InsertQuery.String()
}

// WithKey sets the encryption key for this query
func (q *BunInsertQuery) WithKey(keyID string) *BunInsertQuery {
	q.keyID = keyID
	return q
}

// encryptModel encrypts fields tagged with encrypted:"true"
func (q *BunInsertQuery) encryptModel(model any) error {
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
