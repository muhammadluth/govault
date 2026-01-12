package bun

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/muhammadluth/govault/internal"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/feature"
	"github.com/uptrace/bun/schema"
)

// Ident is an alias for bun.Ident to match official Bun signature
type Ident = bun.Ident

// BunWrapQueries wraps the database with encrypted query methods
func BunWrapQueries(db *bun.DB, govault *internal.GovaultDB) any {
	if err := db.Ping(); err != nil {
		panic(fmt.Sprintf("failed to ping bun.DB: %v", err))
	}
	return &BunDB{
		DB:      db,
		govault: govault,
	}
}

// BunDB wraps bun.DB with encryption support
type BunDB struct {
	*bun.DB
	govault *internal.GovaultDB
	keyID   string // Optional key ID for this query context
}

// BunTx wraps bun.Tx with encryption support
type BunTx struct {
	bun.Tx
	govault *internal.GovaultDB
	keyID   string
}

// --- BunDB Methods ---

// WithKey returns a new BunDB with the specified encryption key
func (db *BunDB) WithKey(keyID string) *BunDB {
	return &BunDB{
		DB:      db.DB,
		govault: db.govault,
		keyID:   keyID,
	}
}

// WithQueryHook returns a copy of the DB with the provided query hook attached.
func (db *BunDB) WithQueryHook(hook bun.QueryHook) *BunDB {
	return &BunDB{
		DB:      db.DB.WithQueryHook(hook),
		govault: db.govault,
		keyID:   db.keyID,
	}
}

// UpdateFQN returns a fully qualified column name.
func (db *BunDB) UpdateFQN(alias, column string) Ident {
	if db.HasFeature(feature.UpdateMultiTable) {
		return Ident(alias + "." + column)
	}
	return Ident(column)
}

// HasFeature uses feature package to report whether the underlying DBMS supports this feature.
func (db *BunDB) HasFeature(feat feature.Feature) bool {
	return db.DB.Dialect().Features().Has(feat)
}

// WithNamedArg returns a copy of the DB with an additional named argument.
func (db *BunDB) WithNamedArg(name string, value any) *BunDB {
	return &BunDB{
		DB:      db.DB.WithNamedArg(name, value),
		govault: db.govault,
		keyID:   db.keyID,
	}
}

// QueryGen returns the query generator
func (db *BunDB) QueryGen() schema.QueryGen {
	return db.DB.QueryGen()
}

// NewInsert creates a new insert query with encryption
func (db *BunDB) NewInsert() *BunInsertQuery {
	return &BunInsertQuery{
		InsertQuery: db.DB.NewInsert(),
		govault:     db.govault,
		keyID:       db.keyID,
	}
}

// NewSelect creates a new select query with decryption
func (db *BunDB) NewSelect() *BunSelectQuery {
	return &BunSelectQuery{
		SelectQuery: db.DB.NewSelect(),
		govault:     db.govault,
	}
}

// NewUpdate creates a new update query with encryption
func (db *BunDB) NewUpdate() *BunUpdateQuery {
	return &BunUpdateQuery{
		UpdateQuery: db.DB.NewUpdate(),
		govault:     db.govault,
		keyID:       db.keyID,
	}
}

// NewDelete creates a new delete query
func (db *BunDB) NewDelete() *BunDeleteQuery {
	return &BunDeleteQuery{
		DeleteQuery: db.DB.NewDelete(),
		govault:     db.govault,
		keyID:       db.keyID,
	}
}

// NewRaw creates a new raw query with encryption/decryption support
func (db *BunDB) NewRaw(query string, args ...any) *BunRawQuery {
	return &BunRawQuery{
		RawQuery: db.DB.NewRaw(query, args...),
		govault:  db.govault,
		keyID:    db.keyID,
	}
}

// NewMerge creates a new merge query
func (db *BunDB) NewMerge() *bun.MergeQuery {
	return db.DB.NewMerge()
}

// NewCreateTable creates a new create table query
func (db *BunDB) NewCreateTable() *bun.CreateTableQuery {
	return db.DB.NewCreateTable()
}

// NewDropTable creates a new drop table query
func (db *BunDB) NewDropTable() *bun.DropTableQuery {
	return db.DB.NewDropTable()
}

// NewTruncateTable creates a new truncate table query
func (db *BunDB) NewTruncateTable() *bun.TruncateTableQuery {
	return db.DB.NewTruncateTable()
}

// NewAddColumn creates a new add column query
func (db *BunDB) NewAddColumn() *bun.AddColumnQuery {
	return db.DB.NewAddColumn()
}

// NewDropColumn creates a new drop column query
func (db *BunDB) NewDropColumn() *bun.DropColumnQuery {
	return db.DB.NewDropColumn()
}

// ResetModel resets the model
func (db *BunDB) ResetModel(ctx context.Context, models ...any) error {
	return db.DB.ResetModel(ctx, models...)
}

// NewCreateIndex creates a new create index query
func (db *BunDB) NewCreateIndex() *bun.CreateIndexQuery {
	return db.DB.NewCreateIndex()
}

// NewDropIndex creates a new drop index query
func (db *BunDB) NewDropIndex() *bun.DropIndexQuery {
	return db.DB.NewDropIndex()
}

// NewValues creates a new values query
func (db *BunDB) NewValues(model any) *bun.ValuesQuery {
	return db.DB.NewValues(model)
}

// Exec executes the query
func (db *BunDB) Exec(query string, args ...any) (sql.Result, error) {
	return db.DB.ExecContext(context.Background(), query, args...)
}

// ExecContext executes the query
func (db *BunDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.DB.ExecContext(ctx, query, args...)
}

// Query executes the query
func (db *BunDB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.DB.QueryContext(context.Background(), query, args...)
}

// QueryContext executes the query
func (db *BunDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.DB.QueryContext(ctx, query, args...)
}

// QueryRow executes the query
func (db *BunDB) QueryRow(query string, args ...any) *sql.Row {
	return db.DB.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes the query
func (db *BunDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.DB.QueryRowContext(ctx, query, args...)
}

// Dialect returns the database dialect
func (db *BunDB) Dialect() schema.Dialect {
	return db.DB.Dialect()
}

// Table returns the table schema
func (db *BunDB) Table(typ reflect.Type) *schema.Table {
	return db.DB.Table(typ)
}

// RegisterModel registers the models
func (db *BunDB) RegisterModel(models ...any) {
	db.DB.RegisterModel(models...)
}

// ScanRow executes the query and scans the result
func (db *BunDB) ScanRow(ctx context.Context, rows *sql.Rows, dest ...any) error {
	err := db.DB.ScanRow(ctx, rows, dest...)
	if err != nil {
		return err
	}
	for _, d := range dest {
		if err := db.govault.DecryptRecursive(d); err != nil {
			return err
		}
	}
	return nil
}

// ScanRows executes the query and scans the result
func (db *BunDB) ScanRows(ctx context.Context, rows *sql.Rows, dest ...any) error {
	err := db.DB.ScanRows(ctx, rows, dest...)
	if err != nil {
		return err
	}
	for _, d := range dest {
		if err := db.govault.DecryptRecursive(d); err != nil {
			return err
		}
	}
	return nil
}

// Begin starts a new transaction
func (db *BunDB) Begin() (*BunTx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &BunTx{
		Tx:      tx,
		govault: db.govault,
		keyID:   db.keyID,
	}, nil
}

// BeginTx starts a new transaction with options
func (db *BunDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*BunTx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &BunTx{
		Tx:      tx,
		govault: db.govault,
		keyID:   db.keyID,
	}, nil
}

// RunInTx runs the function in a transaction
func (db *BunDB) RunInTx(ctx context.Context, opts *sql.TxOptions, f func(context.Context, *BunTx) error) error {
	return db.DB.RunInTx(ctx, opts, func(ctx context.Context, tx bun.Tx) error {
		return f(ctx, &BunTx{
			Tx:      tx,
			govault: db.govault,
			keyID:   db.keyID,
		})
	})
}

// --- BunTx Methods ---

// Commit commits the transaction
func (tx *BunTx) Commit() error {
	return tx.Tx.Commit()
}

// Rollback rolls back the transaction
func (tx *BunTx) Rollback() error {
	return tx.Tx.Rollback()
}

// WithKey returns a new BunTx with the specified encryption key
func (tx *BunTx) WithKey(keyID string) *BunTx {
	return &BunTx{
		Tx:      tx.Tx,
		govault: tx.govault,
		keyID:   keyID,
	}
}

// NewInsert creates a new insert query with encryption
func (tx *BunTx) NewInsert() *BunInsertQuery {
	return &BunInsertQuery{
		InsertQuery: tx.Tx.NewInsert(),
		govault:     tx.govault,
		keyID:       tx.keyID,
	}
}

// NewSelect creates a new select query with decryption
func (tx *BunTx) NewSelect() *BunSelectQuery {
	return &BunSelectQuery{
		SelectQuery: tx.Tx.NewSelect(),
		govault:     tx.govault,
	}
}

// NewUpdate creates a new update query with encryption
func (tx *BunTx) NewUpdate() *BunUpdateQuery {
	return &BunUpdateQuery{
		UpdateQuery: tx.Tx.NewUpdate(),
		govault:     tx.govault,
		keyID:       tx.keyID,
	}
}

// NewDelete creates a new delete query
func (tx *BunTx) NewDelete() *BunDeleteQuery {
	return &BunDeleteQuery{
		DeleteQuery: tx.Tx.NewDelete(),
		govault:     tx.govault,
		keyID:       tx.keyID,
	}
}

// NewRaw creates a new raw query with encryption/decryption support
func (tx *BunTx) NewRaw(query string, args ...any) *BunRawQuery {
	return &BunRawQuery{
		RawQuery: tx.Tx.NewRaw(query, args...),
		govault:  tx.govault,
		keyID:    tx.keyID,
	}
}

// NewMerge creates a new merge query
func (tx *BunTx) NewMerge() *bun.MergeQuery {
	return tx.Tx.NewMerge()
}

// NewCreateTable creates a new create table query
func (tx *BunTx) NewCreateTable() *bun.CreateTableQuery {
	return tx.Tx.NewCreateTable()
}

// NewDropTable creates a new drop table query
func (tx *BunTx) NewDropTable() *bun.DropTableQuery {
	return tx.Tx.NewDropTable()
}

// NewTruncateTable creates a new truncate table query
func (tx *BunTx) NewTruncateTable() *bun.TruncateTableQuery {
	return tx.Tx.NewTruncateTable()
}

// NewAddColumn creates a new add column query
func (tx *BunTx) NewAddColumn() *bun.AddColumnQuery {
	return tx.Tx.NewAddColumn()
}

// NewDropColumn creates a new drop column query
func (tx *BunTx) NewDropColumn() *bun.DropColumnQuery {
	return tx.Tx.NewDropColumn()
}

// NewCreateIndex creates a new create index query
func (tx *BunTx) NewCreateIndex() *bun.CreateIndexQuery {
	return tx.Tx.NewCreateIndex()
}

// NewDropIndex creates a new drop index query
func (tx *BunTx) NewDropIndex() *bun.DropIndexQuery {
	return tx.Tx.NewDropIndex()
}

// NewValues creates a new values query
func (tx *BunTx) NewValues(model any) *bun.ValuesQuery {
	return tx.Tx.NewValues(model)
}

// Exec executes the query
func (tx *BunTx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.Tx.ExecContext(context.Background(), query, args...)
}

// ExecContext executes the query
func (tx *BunTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.Tx.ExecContext(ctx, query, args...)
}

// Query executes the query
func (tx *BunTx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.Tx.QueryContext(context.Background(), query, args...)
}

// QueryContext executes the query
func (tx *BunTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.Tx.QueryContext(ctx, query, args...)
}

// QueryRow executes the query
func (tx *BunTx) QueryRow(query string, args ...any) *sql.Row {
	return tx.Tx.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes the query
func (tx *BunTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.Tx.QueryRowContext(ctx, query, args...)
}

// Dialect returns the database dialect
func (tx *BunTx) Dialect() schema.Dialect {
	return tx.Tx.Dialect()
}

// UpdateFQN returns a fully qualified column name.
func (tx *BunTx) UpdateFQN(alias, column string) Ident {
	if tx.HasFeature(feature.UpdateMultiTable) {
		return Ident(alias + "." + column)
	}
	return Ident(column)
}

// HasFeature uses feature package to report whether the underlying DBMS supports this feature.
func (tx *BunTx) HasFeature(feat feature.Feature) bool {
	return tx.Tx.Dialect().Features().Has(feat)
}

// Begin starts a new transaction point
func (tx *BunTx) Begin() (*BunTx, error) {
	ntx, err := tx.Tx.Begin()
	if err != nil {
		return nil, err
	}
	return &BunTx{
		Tx:      ntx,
		govault: tx.govault,
		keyID:   tx.keyID,
	}, nil
}

// BeginTx starts a new transaction point with options
func (tx *BunTx) BeginTx(ctx context.Context, opts *sql.TxOptions) (*BunTx, error) {
	ntx, err := tx.Tx.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &BunTx{
		Tx:      ntx,
		govault: tx.govault,
		keyID:   tx.keyID,
	}, nil
}

// RunInTx runs the function in a transaction point
func (tx *BunTx) RunInTx(ctx context.Context, opts *sql.TxOptions, f func(context.Context, *BunTx) error) error {
	return tx.Tx.RunInTx(ctx, opts, func(ctx context.Context, ntx bun.Tx) error {
		return f(ctx, &BunTx{
			Tx:      ntx,
			govault: tx.govault,
			keyID:   tx.keyID,
		})
	})
}
