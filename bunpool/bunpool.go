package bunpool

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/muhammadluth/govault"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/schema"
)

// Pool represents a Bun database pool
type Pool struct {
	db        *bun.DB
	encryptor *govault.Encryptor
}

// NewPool creates a new Bun pool
func NewPool(sqldb *sql.DB, dialect schema.Dialect) *Pool {
	return &Pool{
		db: bun.NewDB(sqldb, dialect),
	}
}

// GetName returns the pool name
func (p *Pool) GetName() string {
	return "bun"
}

// SetEncryptor sets the encryptor for this pool
func (p *Pool) SetEncryptor(encryptor *govault.Encryptor) {
	p.encryptor = encryptor
}

// DB returns the underlying bun.DB
func (p *Pool) DB() *bun.DB {
	return p.db
}

// NewInsert creates a new insert query with encryption
func (p *Pool) NewInsert() *InsertQuery {
	return &InsertQuery{
		InsertQuery: p.db.NewInsert(),
		encryptor:   p.encryptor,
	}
}

// NewSelect creates a new select query with decryption
func (p *Pool) NewSelect() *SelectQuery {
	return &SelectQuery{
		SelectQuery: p.db.NewSelect(),
		encryptor:   p.encryptor,
	}
}

// NewUpdate creates a new update query with encryption
func (p *Pool) NewUpdate() *UpdateQuery {
	return &UpdateQuery{
		UpdateQuery: p.db.NewUpdate(),
		encryptor:   p.encryptor,
	}
}

// NewDelete creates a new delete query
func (p *Pool) NewDelete() *bun.DeleteQuery {
	return p.db.NewDelete()
}

// InsertQuery wraps bun.InsertQuery with encryption
type InsertQuery struct {
	*bun.InsertQuery
	encryptor *govault.Encryptor
}

// Model sets the model and encrypts fields
func (q *InsertQuery) Model(model interface{}) *InsertQuery {
	if err := encryptModel(q.encryptor, model); err != nil {
		panic(err)
	}
	q.InsertQuery.Model(model)
	return q
}

// SelectQuery wraps bun.SelectQuery with decryption
type SelectQuery struct {
	*bun.SelectQuery
	encryptor *govault.Encryptor
}

// Model sets the model for select
func (q *SelectQuery) Model(model interface{}) *SelectQuery {
	q.SelectQuery.Model(model)
	return q
}

// WherePK sets the where primary key for select
func (q *SelectQuery) WherePK(cols ...string) *SelectQuery {
	q.SelectQuery.WherePK(cols...)
	return q
}

func (q *SelectQuery) Where(query string, args ...any) *SelectQuery {
	q.SelectQuery.Where(query, args...)
	return q
}

func (q *SelectQuery) WhereOr(query string, args ...any) *SelectQuery {
	q.SelectQuery.WhereOr(query, args...)
	return q
}

func (q *SelectQuery) WhereGroup(sep string, fn func(*SelectQuery) *SelectQuery) *SelectQuery {
	q.SelectQuery.WhereGroup(sep, func(sq *bun.SelectQuery) *bun.SelectQuery {
		return fn(q).SelectQuery
	})
	return q
}

func (q *SelectQuery) WhereDeleted() *SelectQuery {
	q.SelectQuery.WhereDeleted()
	return q
}

func (q *SelectQuery) WhereAllWithDeleted() *SelectQuery {
	q.SelectQuery.WhereAllWithDeleted()
	return q
}

func (q *SelectQuery) Order(orders ...string) *SelectQuery {
	q.SelectQuery.Order(orders...)
	return q
}

func (q *SelectQuery) OrderBy(colName string, sortDir bun.Order) *SelectQuery {
	q.SelectQuery.OrderBy(colName, sortDir)
	return q
}

func (q *SelectQuery) OrderExpr(query string, args ...any) *SelectQuery {
	q.SelectQuery.OrderExpr(query, args...)
	return q
}

func (q *SelectQuery) Limit(n int) *SelectQuery {
	q.SelectQuery.Limit(n)
	return q
}

func (q *SelectQuery) Offset(n int) *SelectQuery {
	q.SelectQuery.Offset(n)
	return q
}

// Scan executes the query and decrypts results
func (q *SelectQuery) Scan(ctx context.Context, dest ...interface{}) error {
	err := q.SelectQuery.Scan(ctx, dest...)
	if err != nil {
		return err
	}

	// Decrypt all destination values
	for _, d := range dest {
		if err := decryptValue(q.encryptor, d); err != nil {
			return err
		}
	}

	return nil
}

// decryptValue handles decryption for various types (single model, slice, etc)
func decryptValue(encryptor *govault.Encryptor, value interface{}) error {
	if value == nil {
		return nil
	}

	val := reflect.ValueOf(value)
	if val.Kind() != reflect.Ptr {
		return nil
	}

	val = val.Elem()

	// Handle slice
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Ptr {
				if err := decryptModel(encryptor, elem.Interface()); err != nil {
					return err
				}
			} else if elem.Kind() == reflect.Struct {
				if elem.CanAddr() {
					if err := decryptModel(encryptor, elem.Addr().Interface()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// Handle single struct
	if val.Kind() == reflect.Struct {
		return decryptModel(encryptor, value)
	}

	return nil
}

// UpdateQuery wraps bun.UpdateQuery with encryption
type UpdateQuery struct {
	*bun.UpdateQuery
	encryptor *govault.Encryptor
}

// Model sets the model and encrypts fields
func (q *UpdateQuery) Model(model interface{}) *UpdateQuery {
	if err := encryptModel(q.encryptor, model); err != nil {
		panic(err)
	}
	q.UpdateQuery.Model(model)
	return q
}

// encryptModel encrypts fields tagged with encrypted:"true"
func encryptModel(encryptor *govault.Encryptor, model interface{}) error {
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

		if fieldType.Tag.Get("encrypted") == "true" {
			if field.Kind() == reflect.String && field.CanSet() {
				plaintext := field.String()
				if plaintext != "" {
					encrypted, err := encryptor.Encrypt(plaintext)
					if err != nil {
						return err
					}
					field.SetString(encrypted)
				}
			}
		}
	}

	return nil
}

// decryptModel decrypts fields tagged with encrypted:"true"
func decryptModel(encryptor *govault.Encryptor, model interface{}) error {
	if model == nil {
		return nil
	}

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

		// Skip if not exported
		if !field.CanSet() {
			continue
		}

		if fieldType.Tag.Get("encrypted") == "true" {
			if field.Kind() == reflect.String {
				ciphertext := field.String()
				if ciphertext != "" && strings.Contains(ciphertext, "|") {

					decrypted, err := encryptor.Decrypt(ciphertext)
					if err != nil {
						return fmt.Errorf("failed to decrypt field %s: %w", fieldType.Name, err)
					}
					field.SetString(decrypted)
				}
			}
		}
	}

	return nil
}
