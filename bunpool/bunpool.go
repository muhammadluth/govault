package bunpool

import (
	"context"
	"database/sql"
	"reflect"

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

// Scan executes the query and decrypts results
func (q *SelectQuery) Scan(ctx context.Context, dest ...interface{}) error {
	err := q.SelectQuery.Scan(ctx, dest...)
	if err != nil {
		return err
	}

	for _, d := range dest {
		if err := decryptModel(q.encryptor, d); err != nil {
			return err
		}
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
				ciphertext := field.String()
				if ciphertext != "" {
					decrypted, err := encryptor.Decrypt(ciphertext)
					if err != nil {
						return err
					}
					field.SetString(decrypted)
				}
			}
		}
	}

	return nil
}
