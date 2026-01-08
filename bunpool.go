// Package govault - Bun adapter implementation
package govault

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/schema"
)

// BunAdapter implements Adapter for Bun ORM
type BunAdapter struct {
	sqldb   *sql.DB
	dialect schema.Dialect
}

// newBunAdapter creates a new Bun adapter
func newBunAdapter(sqldb *sql.DB) (Adapter, error) {
	return &BunAdapter{
		sqldb:   sqldb,
		dialect: pgdialect.New(),
	}, nil
}

// GetName returns the adapter name
func (a *BunAdapter) GetName() string {
	return "bun"
}

// WrapQueries wraps the database with encrypted query methods
func (a *BunAdapter) WrapQueries(db *DB) interface{} {
	bunDB := bun.NewDB(a.sqldb, a.dialect)
	return &BunDB{
		DB:      bunDB,
		govault: db,
	}
}

// BunDB wraps bun.DB with encryption support
type BunDB struct {
	*bun.DB
	govault *DB
	keyID   string // Optional key ID for this query context
}

// WithKey returns a new BunDB with the specified encryption key
func (db *BunDB) WithKey(keyID string) *BunDB {
	return &BunDB{
		DB:      db.DB,
		govault: db.govault,
		keyID:   keyID,
	}
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

// NewCreateTable creates a new create table query
func (db *BunDB) NewCreateTable() *bun.CreateTableQuery {
	return db.DB.NewCreateTable()
}

// NewDropTable creates a new drop table query
func (db *BunDB) NewDropTable() *bun.DropTableQuery {
	return db.DB.NewDropTable()
}

// NewCreateIndex creates a new create index query
func (db *BunDB) NewCreateIndex() *bun.CreateIndexQuery {
	return db.DB.NewCreateIndex()
}

// NewDropIndex creates a new drop index query
func (db *BunDB) NewDropIndex() *bun.DropIndexQuery {
	return db.DB.NewDropIndex()
}

// NewTruncateTable creates a new truncate table query
func (db *BunDB) NewTruncateTable() *bun.TruncateTableQuery {
	return db.DB.NewTruncateTable()
}

// NewRaw creates a new raw query with encryption/decryption support
func (db *BunDB) NewRaw(query string, args ...interface{}) *BunRawQuery {
	return &BunRawQuery{
		RawQuery: db.DB.NewRaw(query, args...),
		govault:  db.govault,
		keyID:    db.keyID,
	}
}

// NewValues creates a new values query
func (db *BunDB) NewValues(model interface{}) *bun.ValuesQuery {
	return db.DB.NewValues(model)
}

// BunRawQuery wraps bun.RawQuery with encryption/decryption support
type BunRawQuery struct {
	*bun.RawQuery
	govault *DB
	keyID   string
}

// WithKey sets the encryption key for this raw query
func (q *BunRawQuery) WithKey(keyID string) *BunRawQuery {
	q.keyID = keyID
	return q
}

// EncryptValue encrypts a single value for use in raw SQL
// Returns encrypted string in format: keyID|nonce|ciphertext
func (q *BunRawQuery) EncryptValue(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}

	if q.keyID != "" {
		return q.govault.Encrypt(plaintext, q.keyID)
	}
	return q.govault.Encrypt(plaintext)
}

// Exec executes the raw query
func (q *BunRawQuery) Exec(ctx context.Context, dest ...interface{}) (sql.Result, error) {
	return q.RawQuery.Exec(ctx, dest...)
}

// Scan executes the raw query and scans results
// If dest is a struct with encrypted fields, they will be decrypted
func (q *BunRawQuery) Scan(ctx context.Context, dest ...interface{}) error {
	err := q.RawQuery.Scan(ctx, dest...)
	if err != nil {
		return err
	}

	// Attempt to decrypt if dest contains encrypted fields
	for _, d := range dest {
		if err := q.decryptScanResult(d); err != nil {
			return err
		}
	}

	return nil
}

// decryptScanResult attempts to decrypt encrypted fields in the scanned result
func (q *BunRawQuery) decryptScanResult(value interface{}) error {
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
				if err := q.decryptStruct(elem.Interface()); err != nil {
					return err
				}
			} else if elem.Kind() == reflect.Struct {
				if elem.CanAddr() {
					if err := q.decryptStruct(elem.Addr().Interface()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// Handle single struct
	if val.Kind() == reflect.Struct {
		return q.decryptStruct(value)
	}

	return nil
}

// decryptStruct decrypts fields tagged with encrypted:"true"
func (q *BunRawQuery) decryptStruct(model interface{}) error {
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

		if !field.CanSet() {
			continue
		}

		if fieldType.Tag.Get("encrypted") == "true" {
			if field.Kind() == reflect.String {
				ciphertext := field.String()
				if ciphertext != "" && strings.Contains(ciphertext, "|") {
					decrypted, err := q.govault.Decrypt(ciphertext)
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

// BunInsertQuery wraps bun.InsertQuery
type BunInsertQuery struct {
	*bun.InsertQuery
	govault *DB
	keyID   string
}

// Conn sets the database connection
func (q *BunInsertQuery) Conn(db bun.IConn) *BunInsertQuery {
	q.InsertQuery.Conn(db)
	return q
}

// Model sets the model and encrypts fields
func (q *BunInsertQuery) Model(model interface{}) *BunInsertQuery {
	if err := q.encryptModel(model); err != nil {
		panic(err)
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

// On adds an ON CONFLICT clause (PostgreSQL)
func (q *BunInsertQuery) On(query string, args ...any) *BunInsertQuery {
	q.InsertQuery.On(query, args...)
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

// Exec executes the insert query
func (q *BunInsertQuery) Exec(ctx context.Context, dest ...interface{}) (sql.Result, error) {
	return q.InsertQuery.Exec(ctx, dest...)
}

// Scan executes the query and scans the result
func (q *BunInsertQuery) Scan(ctx context.Context, dest ...interface{}) error {
	return q.InsertQuery.Scan(ctx, dest...)
}

// WithKey sets the encryption key for this query
func (q *BunInsertQuery) WithKey(keyID string) *BunInsertQuery {
	q.keyID = keyID
	return q
}

// encryptModel encrypts fields tagged with encrypted:"true"
func (q *BunInsertQuery) encryptModel(model interface{}) error {
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

// BunSelectQuery wraps bun.SelectQuery
type BunSelectQuery struct {
	*bun.SelectQuery
	govault *DB
}

// Conn sets the database connection
func (q *BunSelectQuery) Conn(db bun.IConn) *BunSelectQuery {
	q.SelectQuery.Conn(db)
	return q
}

// Model sets the model for select
func (q *BunSelectQuery) Model(model interface{}) *BunSelectQuery {
	q.SelectQuery.Model(model)
	return q
}

// Err sets an error on the query
func (q *BunSelectQuery) Err(err error) *BunSelectQuery {
	q.SelectQuery.Err(err)
	return q
}

// Apply applies functions to the query
func (q *BunSelectQuery) Apply(fns ...func(*BunSelectQuery) *BunSelectQuery) *BunSelectQuery {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// With adds a WITH clause (Common Table Expression)
func (q *BunSelectQuery) With(name string, query bun.Query) *BunSelectQuery {
	q.SelectQuery.With(name, query)
	return q
}

// WithRecursive adds a WITH RECURSIVE clause
func (q *BunSelectQuery) WithRecursive(name string, query bun.Query) *BunSelectQuery {
	q.SelectQuery.WithRecursive(name, query)
	return q
}

// Distinct adds a DISTINCT clause
func (q *BunSelectQuery) Distinct() *BunSelectQuery {
	q.SelectQuery.Distinct()
	return q
}

// DistinctOn adds a DISTINCT ON clause (PostgreSQL)
func (q *BunSelectQuery) DistinctOn(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.DistinctOn(query, args...)
	return q
}

// Table specifies table(s) to select from
func (q *BunSelectQuery) Table(tables ...string) *BunSelectQuery {
	q.SelectQuery.Table(tables...)
	return q
}

// TableExpr adds a table expression
func (q *BunSelectQuery) TableExpr(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.TableExpr(query, args...)
	return q
}

// ModelTableExpr overrides the table name from model
func (q *BunSelectQuery) ModelTableExpr(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.ModelTableExpr(query, args...)
	return q
}

// Column adds columns to SELECT
func (q *BunSelectQuery) Column(columns ...string) *BunSelectQuery {
	q.SelectQuery.Column(columns...)
	return q
}

// ColumnExpr adds a column expression
func (q *BunSelectQuery) ColumnExpr(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.ColumnExpr(query, args...)
	return q
}

// ExcludeColumn excludes columns from being selected
func (q *BunSelectQuery) ExcludeColumn(columns ...string) *BunSelectQuery {
	q.SelectQuery.ExcludeColumn(columns...)
	return q
}

// WherePK sets the where primary key for select
func (q *BunSelectQuery) WherePK(cols ...string) *BunSelectQuery {
	q.SelectQuery.WherePK(cols...)
	return q
}

func (q *BunSelectQuery) Where(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.Where(query, args...)
	return q
}

func (q *BunSelectQuery) WhereOr(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.WhereOr(query, args...)
	return q
}

func (q *BunSelectQuery) WhereGroup(sep string, fn func(*BunSelectQuery) *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.WhereGroup(sep, func(sq *bun.SelectQuery) *bun.SelectQuery {
		return fn(q).SelectQuery
	})
	return q
}

func (q *BunSelectQuery) WhereDeleted() *BunSelectQuery {
	q.SelectQuery.WhereDeleted()
	return q
}

func (q *BunSelectQuery) WhereAllWithDeleted() *BunSelectQuery {
	q.SelectQuery.WhereAllWithDeleted()
	return q
}

// Join adds a JOIN clause
func (q *BunSelectQuery) Join(join string, args ...any) *BunSelectQuery {
	q.SelectQuery.Join(join, args...)
	return q
}

// JoinOn adds an ON condition to the most recent JOIN
func (q *BunSelectQuery) JoinOn(cond string, args ...any) *BunSelectQuery {
	q.SelectQuery.JoinOn(cond, args...)
	return q
}

// JoinOnOr adds an ON condition with OR
func (q *BunSelectQuery) JoinOnOr(cond string, args ...any) *BunSelectQuery {
	q.SelectQuery.JoinOnOr(cond, args...)
	return q
}

// Group adds columns to GROUP BY
func (q *BunSelectQuery) Group(columns ...string) *BunSelectQuery {
	q.SelectQuery.Group(columns...)
	return q
}

// GroupExpr adds a GROUP BY expression
func (q *BunSelectQuery) GroupExpr(group string, args ...any) *BunSelectQuery {
	q.SelectQuery.GroupExpr(group, args...)
	return q
}

// Having adds a HAVING clause
func (q *BunSelectQuery) Having(having string, args ...any) *BunSelectQuery {
	q.SelectQuery.Having(having, args...)
	return q
}

func (q *BunSelectQuery) Order(orders ...string) *BunSelectQuery {
	q.SelectQuery.Order(orders...)
	return q
}

func (q *BunSelectQuery) OrderBy(colName string, sortDir bun.Order) *BunSelectQuery {
	q.SelectQuery.OrderBy(colName, sortDir)
	return q
}

func (q *BunSelectQuery) OrderExpr(query string, args ...any) *BunSelectQuery {
	q.SelectQuery.OrderExpr(query, args...)
	return q
}

func (q *BunSelectQuery) Limit(n int) *BunSelectQuery {
	q.SelectQuery.Limit(n)
	return q
}

func (q *BunSelectQuery) Offset(n int) *BunSelectQuery {
	q.SelectQuery.Offset(n)
	return q
}

// For adds a FOR clause for row locking (e.g., "UPDATE", "SHARE")
func (q *BunSelectQuery) For(s string, args ...any) *BunSelectQuery {
	q.SelectQuery.For(s, args...)
	return q
}

// Union combines queries with UNION
func (q *BunSelectQuery) Union(other *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.Union(other.SelectQuery)
	return q
}

// UnionAll combines queries with UNION ALL
func (q *BunSelectQuery) UnionAll(other *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.UnionAll(other.SelectQuery)
	return q
}

// Intersect returns rows in both queries
func (q *BunSelectQuery) Intersect(other *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.Intersect(other.SelectQuery)
	return q
}

// IntersectAll returns rows in both queries (with duplicates)
func (q *BunSelectQuery) IntersectAll(other *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.IntersectAll(other.SelectQuery)
	return q
}

// Except returns rows in this query but not in other
func (q *BunSelectQuery) Except(other *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.Except(other.SelectQuery)
	return q
}

// ExceptAll returns rows in this query but not in other (with duplicates)
func (q *BunSelectQuery) ExceptAll(other *BunSelectQuery) *BunSelectQuery {
	q.SelectQuery.ExceptAll(other.SelectQuery)
	return q
}

// Relation adds a relation to the query
func (q *BunSelectQuery) Relation(name string, apply ...func(*BunSelectQuery) *BunSelectQuery) *BunSelectQuery {
	if len(apply) > 1 {
		panic("only one apply function is supported")
	}

	if len(apply) == 0 {
		q.SelectQuery.Relation(name)
	} else {
		q.SelectQuery.Relation(name, func(sq *bun.SelectQuery) *bun.SelectQuery {
			wrapped := &BunSelectQuery{SelectQuery: sq, govault: q.govault}
			return apply[0](wrapped).SelectQuery
		})
	}
	return q
}

// UseIndex adds a USE INDEX hint (MySQL)
func (q *BunSelectQuery) UseIndex(indexes ...string) *BunSelectQuery {
	q.SelectQuery.UseIndex(indexes...)
	return q
}

// UseIndexForJoin adds a USE INDEX FOR JOIN hint (MySQL)
func (q *BunSelectQuery) UseIndexForJoin(indexes ...string) *BunSelectQuery {
	q.SelectQuery.UseIndexForJoin(indexes...)
	return q
}

// UseIndexForOrderBy adds a USE INDEX FOR ORDER BY hint (MySQL)
func (q *BunSelectQuery) UseIndexForOrderBy(indexes ...string) *BunSelectQuery {
	q.SelectQuery.UseIndexForOrderBy(indexes...)
	return q
}

// UseIndexForGroupBy adds a USE INDEX FOR GROUP BY hint (MySQL)
func (q *BunSelectQuery) UseIndexForGroupBy(indexes ...string) *BunSelectQuery {
	q.SelectQuery.UseIndexForGroupBy(indexes...)
	return q
}

// IgnoreIndex adds an IGNORE INDEX hint (MySQL)
func (q *BunSelectQuery) IgnoreIndex(indexes ...string) *BunSelectQuery {
	q.SelectQuery.IgnoreIndex(indexes...)
	return q
}

// IgnoreIndexForJoin adds an IGNORE INDEX FOR JOIN hint (MySQL)
func (q *BunSelectQuery) IgnoreIndexForJoin(indexes ...string) *BunSelectQuery {
	q.SelectQuery.IgnoreIndexForJoin(indexes...)
	return q
}

// IgnoreIndexForOrderBy adds an IGNORE INDEX FOR ORDER BY hint (MySQL)
func (q *BunSelectQuery) IgnoreIndexForOrderBy(indexes ...string) *BunSelectQuery {
	q.SelectQuery.IgnoreIndexForOrderBy(indexes...)
	return q
}

// IgnoreIndexForGroupBy adds an IGNORE INDEX FOR GROUP BY hint (MySQL)
func (q *BunSelectQuery) IgnoreIndexForGroupBy(indexes ...string) *BunSelectQuery {
	q.SelectQuery.IgnoreIndexForGroupBy(indexes...)
	return q
}

// ForceIndex adds a FORCE INDEX hint (MySQL)
func (q *BunSelectQuery) ForceIndex(indexes ...string) *BunSelectQuery {
	q.SelectQuery.ForceIndex(indexes...)
	return q
}

// ForceIndexForJoin adds a FORCE INDEX FOR JOIN hint (MySQL)
func (q *BunSelectQuery) ForceIndexForJoin(indexes ...string) *BunSelectQuery {
	q.SelectQuery.ForceIndexForJoin(indexes...)
	return q
}

// ForceIndexForOrderBy adds a FORCE INDEX FOR ORDER BY hint (MySQL)
func (q *BunSelectQuery) ForceIndexForOrderBy(indexes ...string) *BunSelectQuery {
	q.SelectQuery.ForceIndexForOrderBy(indexes...)
	return q
}

// ForceIndexForGroupBy adds a FORCE INDEX FOR GROUP BY hint (MySQL)
func (q *BunSelectQuery) ForceIndexForGroupBy(indexes ...string) *BunSelectQuery {
	q.SelectQuery.ForceIndexForGroupBy(indexes...)
	return q
}

// Comment adds a comment to the query
func (q *BunSelectQuery) Comment(comment string) *BunSelectQuery {
	q.SelectQuery.Comment(comment)
	return q
}

// Count returns the count of rows
func (q *BunSelectQuery) Count(ctx context.Context) (int, error) {
	return q.SelectQuery.Count(ctx)
}

// Exists checks if any rows match the query
func (q *BunSelectQuery) Exists(ctx context.Context) (bool, error) {
	return q.SelectQuery.Exists(ctx)
}

// ScanAndCount scans results and returns count
func (q *BunSelectQuery) ScanAndCount(ctx context.Context, dest ...interface{}) (int, error) {
	count, err := q.SelectQuery.ScanAndCount(ctx, dest...)
	if err != nil {
		return count, err
	}

	for _, d := range dest {
		if err := q.decryptValue(d); err != nil {
			return count, err
		}
	}

	return count, nil
}

// Scan executes the query and decrypts results
func (q *BunSelectQuery) Scan(ctx context.Context, dest ...interface{}) error {
	err := q.SelectQuery.Scan(ctx, dest...)
	if err != nil {
		return err
	}

	for _, d := range dest {
		if err := q.decryptValue(d); err != nil {
			return err
		}
	}

	return nil
}

// decryptValue handles decryption for various types
func (q *BunSelectQuery) decryptValue(value interface{}) error {
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
				if err := q.decryptModel(elem.Interface()); err != nil {
					return err
				}
			} else if elem.Kind() == reflect.Struct {
				if elem.CanAddr() {
					if err := q.decryptModel(elem.Addr().Interface()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// Handle single struct
	if val.Kind() == reflect.Struct {
		return q.decryptModel(value)
	}

	return nil
}

// decryptModel decrypts fields tagged with encrypted:"true"
func (q *BunSelectQuery) decryptModel(model interface{}) error {
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

		if !field.CanSet() {
			continue
		}

		if fieldType.Tag.Get("encrypted") == "true" {
			if field.Kind() == reflect.String {
				ciphertext := field.String()
				if ciphertext != "" && strings.Contains(ciphertext, "|") {
					decrypted, err := q.govault.Decrypt(ciphertext)
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

// BunUpdateQuery wraps bun.UpdateQuery
type BunUpdateQuery struct {
	*bun.UpdateQuery
	govault *DB
	keyID   string
}

// Conn sets the database connection
func (q *BunUpdateQuery) Conn(db bun.IConn) *BunUpdateQuery {
	q.UpdateQuery.Conn(db)
	return q
}

// Model sets the model and encrypts fields
func (q *BunUpdateQuery) Model(model interface{}) *BunUpdateQuery {
	if err := q.encryptModel(model); err != nil {
		panic(err)
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
func (q *BunUpdateQuery) Exec(ctx context.Context, dest ...interface{}) (sql.Result, error) {
	return q.UpdateQuery.Exec(ctx, dest...)
}

// Scan executes the query and scans the result
func (q *BunUpdateQuery) Scan(ctx context.Context, dest ...interface{}) error {
	return q.UpdateQuery.Scan(ctx, dest...)
}

// WithKey sets the encryption key for this query
func (q *BunUpdateQuery) WithKey(keyID string) *BunUpdateQuery {
	q.keyID = keyID
	return q
}

// encryptModel encrypts fields tagged with encrypted:"true"
func (q *BunUpdateQuery) encryptModel(model interface{}) error {
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
