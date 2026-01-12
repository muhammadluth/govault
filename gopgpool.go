package govault

// import (
// 	"context"
// 	"fmt"
// 	"reflect"

// 	"github.com/go-pg/pg/v10"
// 	"github.com/go-pg/pg/v10/orm"
// )

// // GoPgWrapQueries wraps the database with encrypted query methods
// func GoPgWrapQueries(db *pg.DB, govault *GovaultDB) interface{} {
// 	return &GoPgDB{
// 		DB:      db,
// 		govault: govault,
// 	}
// }

// // GoPgDB wraps pg.DB with encryption support
// type GoPgDB struct {
// 	*pg.DB
// 	govault *GovaultDB
// }

// // Model returns a new query for the model with encryption support
// func (db *GoPgDB) Model(model ...interface{}) *GoPgQuery {
// 	var m interface{}
// 	if len(model) > 0 {
// 		m = model[0]
// 	}
// 	return &GoPgQuery{
// 		Query:   db.DB.Model(model...),
// 		govault: db.govault,
// 		model:   m,
// 	}
// }

// // ModelContext returns a new query for the model with encryption support
// func (db *GoPgDB) ModelContext(ctx context.Context, model ...interface{}) *GoPgQuery {
// 	var m interface{}
// 	if len(model) > 0 {
// 		m = model[0]
// 	}
// 	return &GoPgQuery{
// 		Query:   db.DB.ModelContext(ctx, model...),
// 		govault: db.govault,
// 		model:   m,
// 	}
// }

// // RunInTransaction runs a function in a transaction
// func (db *GoPgDB) RunInTransaction(ctx context.Context, fn func(*GoPgTx) error) error {
// 	return db.DB.RunInTransaction(ctx, func(tx *pg.Tx) error {
// 		return fn(&GoPgTx{
// 			Tx:      tx,
// 			govault: db.govault,
// 		})
// 	})
// }

// // Begin begins a transaction
// func (db *GoPgDB) Begin() (*GoPgTx, error) {
// 	tx, err := db.DB.Begin()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &GoPgTx{
// 		Tx:      tx,
// 		govault: db.govault,
// 	}, nil
// }

// // GoPgTx wraps pg.Tx
// type GoPgTx struct {
// 	*pg.Tx
// 	govault *GovaultDB
// }

// // Model returns a new query for the model with encryption support
// func (tx *GoPgTx) Model(model ...interface{}) *GoPgQuery {
// 	var m interface{}
// 	if len(model) > 0 {
// 		m = model[0]
// 	}
// 	return &GoPgQuery{
// 		Query:   tx.Tx.Model(model...),
// 		govault: tx.govault,
// 		model:   m,
// 	}
// }

// // ModelContext returns a new query for the model with encryption support
// func (tx *GoPgTx) ModelContext(ctx context.Context, model ...interface{}) *GoPgQuery {
// 	var m interface{}
// 	if len(model) > 0 {
// 		m = model[0]
// 	}
// 	return &GoPgQuery{
// 		Query:   tx.Tx.ModelContext(ctx, model...),
// 		govault: tx.govault,
// 		model:   m,
// 	}
// }

// // GoPgQuery wraps orm.Query
// type GoPgQuery struct {
// 	*orm.Query
// 	govault *GovaultDB
// 	model   interface{}
// 	keyID   string
// }

// // WithKey sets the encryption key for this query
// func (q *GoPgQuery) WithKey(keyID string) *GoPgQuery {
// 	q.keyID = keyID
// 	return q
// }

// // encryptModel encrypts fields tagged with encrypted:"true"
// func (q *GoPgQuery) encryptModel(model interface{}) error {
// 	val := reflect.ValueOf(model)
// 	if val.Kind() == reflect.Ptr {
// 		val = val.Elem()
// 	}

// 	if val.Kind() != reflect.Struct {
// 		return nil
// 	}

// 	typ := val.Type()
// 	for i := 0; i < val.NumField(); i++ {
// 		field := val.Field(i)
// 		fieldType := typ.Field(i)

// 		if !field.CanSet() {
// 			continue
// 		}

// 		if fieldType.Tag.Get("encrypted") == "true" {
// 			if field.Kind() == reflect.String {
// 				plaintext := field.String()
// 				if plaintext != "" {
// 					var encrypted string
// 					var err error

// 					if q.keyID != "" {
// 						encrypted, err = q.govault.Encrypt(plaintext, q.keyID)
// 					} else {
// 						encrypted, err = q.govault.Encrypt(plaintext)
// 					}

// 					if err != nil {
// 						return fmt.Errorf("failed to encrypt field %s: %w", fieldType.Name, err)
// 					}
// 					field.SetString(encrypted)
// 				}
// 			}
// 		}
// 	}

// 	return nil
// }

// // Insert inserts the model with encryption
// func (q *GoPgQuery) Insert(values ...interface{}) (orm.Result, error) {
// 	if q.model != nil {
// 		if err := q.encryptModel(q.model); err != nil {
// 			return nil, err
// 		}
// 	}
// 	for _, v := range values {
// 		if err := q.encryptModel(v); err != nil {
// 			return nil, err
// 		}
// 	}
// 	return q.Query.Insert(values...)
// }

// // Select selects the model with decryption
// // usage of Select in go-pg: err := db.Model(&user).Select()
// func (q *GoPgQuery) Select(values ...interface{}) error {
// 	err := q.Query.Select(values...)
// 	if err != nil {
// 		return err
// 	}

// 	if q.model != nil {
// 		if err := q.govault.decryptRecursive(q.model); err != nil {
// 			return err
// 		}
// 	}
// 	// Note: values passed to Select() are usually column names, but can be destinations in some contexts?
// 	// In go-pg, Select(columns...) selects columns.
// 	// If Select() is called without args, it selects all columns into the model.

// 	return nil
// }

// // SelectAndCount selects and counts
// func (q *GoPgQuery) SelectAndCount(values ...interface{}) (int, error) {
// 	count, err := q.Query.SelectAndCount(values...)
// 	if err != nil {
// 		return count, err
// 	}

// 	if q.model != nil {
// 		if err := q.govault.decryptRecursive(q.model); err != nil {
// 			return count, err
// 		}
// 	}
// 	return count, nil
// }

// // Update updates the model
// func (q *GoPgQuery) Update(scan ...interface{}) (orm.Result, error) {
// 	if q.model != nil {
// 		if err := q.encryptModel(q.model); err != nil {
// 			return nil, err
// 		}
// 	}
// 	// scan args in Update are usually returning destinations?
// 	// Or sometimes values to update if not using model?
// 	// go-pg: Update(scan ...interface{})
// 	// documentation says: Update updates the model.

// 	res, err := q.Query.Update(scan...)
// 	if err != nil {
// 		return res, err
// 	}

// 	// If scan args provided (RETURNING), decrypt them
// 	for _, s := range scan {
// 		if err := q.govault.decryptRecursive(s); err != nil {
// 			return res, err
// 		}
// 	}

// 	return res, nil
// }

// // UpdateNotZero - similar to Update but omits zero values
// func (q *GoPgQuery) UpdateNotZero(scan ...interface{}) (orm.Result, error) {
// 	if q.model != nil {
// 		if err := q.encryptModel(q.model); err != nil {
// 			return nil, err
// 		}
// 	}

// 	res, err := q.Query.UpdateNotZero(scan...)
// 	if err != nil {
// 		return res, err
// 	}

// 	for _, s := range scan {
// 		if err := q.govault.decryptRecursive(s); err != nil {
// 			return res, err
// 		}
// 	}

// 	return res, nil
// }

// // Delete deletes the model
// func (q *GoPgQuery) Delete(values ...interface{}) (orm.Result, error) {
// 	// Usually we don't encrypt for delete, unless we are deleting by encrypted value (which implies Where clause).
// 	// We don't touch Where clause encryption here (it's hard to parse expressions).

// 	res, err := q.Query.Delete(values...)
// 	if err != nil {
// 		return res, err
// 	}

// 	// If values provided (RETURNING), decrypt
// 	// Wait, Delete(values...) -> values are usually model? Or returning?
// 	// go-pg Delete(values...) -> documentation says "Delete deletes the model. ... if values are present, they are used as returning destinations."
// 	if len(values) > 0 {
// 		for _, v := range values {
// 			if err := q.govault.decryptRecursive(v); err != nil {
// 				return res, err
// 			}
// 		}
// 	}

// 	return res, nil
// }

// // Relation - need to handle relations/joins for recursion
// func (q *GoPgQuery) Relation(name string, apply ...func(*orm.Query) (*orm.Query, error)) *GoPgQuery {
// 	// We can't easily wrap the apply function because it expects *orm.Query
// 	// But `decryptRecursive` handles nested structs so we might not need to intercept Relation
// 	// providing `q.model` is the root model and `go-pg` populates it including relations.
// 	// Since we use `decryptRecursive`, it WILL descend into relations populated by go-pg.
// 	// So we just pass through.
// 	q.Query.Relation(name, apply...)
// 	return q
// }

// // Raw wrapper?
// // go-pg has db.Query(model, query, params...) and db.Exec(query, params...)
// // We should wrap those on DB level.

// func (db *GoPgDB) Exec(query interface{}, params ...interface{}) (orm.Result, error) {
// 	return db.DB.Exec(query, params...)
// }

// func (db *GoPgDB) ExecContext(ctx context.Context, query interface{}, params ...interface{}) (orm.Result, error) {
// 	return db.DB.ExecContext(ctx, query, params...)
// }

// func (db *GoPgDB) Query(model, query interface{}, params ...interface{}) (orm.Result, error) {
// 	res, err := db.DB.Query(model, query, params...)
// 	if err != nil {
// 		return res, err
// 	}
// 	// Decrypt model
// 	if model != nil {
// 		if err := db.govault.decryptRecursive(model); err != nil {
// 			return res, err
// 		}
// 	}
// 	return res, nil
// }

// func (db *GoPgDB) QueryContext(ctx context.Context, model, query interface{}, params ...interface{}) (orm.Result, error) {
// 	res, err := db.DB.QueryContext(ctx, model, query, params...)
// 	if err != nil {
// 		return res, err
// 	}
// 	if model != nil {
// 		if err := db.govault.decryptRecursive(model); err != nil {
// 			return res, err
// 		}
// 	}
// 	return res, nil
// }
