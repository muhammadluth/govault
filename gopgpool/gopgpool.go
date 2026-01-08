package gopgpool

// import (
// 	"context"
// 	"reflect"

// 	"github.com/go-pg/pg/v10"
// 	"github.com/go-pg/pg/v10/orm"
// 	"github.com/muhammadluth/govault"
// )

// // Pool represents a go-pg database pool
// type Pool struct {
// 	db        *pg.DB
// 	encryptor *govault.Encryptor
// }

// // NewPool creates a new go-pg pool
// func NewPool(db *pg.DB) *Pool {
// 	return &Pool{
// 		db: db,
// 	}
// }

// // GetName returns the pool name
// func (p *Pool) GetName() string {
// 	return "go-pg"
// }

// // SetEncryptor sets the encryptor for this pool
// func (p *Pool) SetEncryptor(encryptor *govault.Encryptor) {
// 	p.encryptor = encryptor
// }

// // DB returns the underlying pg.DB
// func (p *Pool) DB() *pg.DB {
// 	return p.db
// }

// // ModelContext returns a new query for the model with encryption support
// func (p *Pool) ModelContext(ctx context.Context, model ...interface{}) *Query {
// 	return &Query{
// 		Query:     p.db.ModelContext(ctx, model...),
// 		encryptor: p.encryptor,
// 		model:     getFirstModel(model),
// 	}
// }

// // Model returns a new query for the model with encryption support
// func (p *Pool) Model(model ...interface{}) *Query {
// 	return p.ModelContext(context.Background(), model...)
// }

// // getFirstModel extracts the first model from variadic parameters
// func getFirstModel(models []interface{}) interface{} {
// 	if len(models) > 0 {
// 		return models[0]
// 	}
// 	return nil
// }

// // Query wraps pg.Query with encryption/decryption support
// type Query struct {
// 	*orm.Query
// 	encryptor *govault.Encryptor
// 	model     interface{} // Store model reference for encryption/decryption
// }

// // Insert inserts the model with encryption
// func (q *Query) Insert(values ...interface{}) (orm.Result, error) {
// 	// Encrypt model if exists
// 	if q.model != nil {
// 		if err := encryptModel(q.encryptor, q.model); err != nil {
// 			return nil, err
// 		}
// 	}

// 	// Encrypt additional values
// 	for _, v := range values {
// 		if err := encryptModel(q.encryptor, v); err != nil {
// 			return nil, err
// 		}
// 	}

// 	return q.Query.Insert(values...)
// }

// // Update updates the model with encryption
// func (q *Query) Update(scan ...interface{}) (orm.Result, error) {
// 	// Encrypt model if exists
// 	if q.model != nil {
// 		if err := encryptModel(q.encryptor, q.model); err != nil {
// 			return nil, err
// 		}
// 	}

// 	return q.Query.Update(scan...)
// }

// // UpdateNotZero updates the model with encryption
// func (q *Query) UpdateNotZero(scan ...interface{}) (orm.Result, error) {
// 	// Encrypt model if exists
// 	if q.model != nil {
// 		if err := encryptModel(q.encryptor, q.model); err != nil {
// 			return nil, err
// 		}
// 	}

// 	return q.Query.UpdateNotZero(scan...)
// }

// // Select selects the model with decryption
// func (q *Query) Select(values ...interface{}) error {
// 	err := q.Query.Select(values...)
// 	if err != nil {
// 		return err
// 	}

// 	// Decrypt model if exists
// 	if q.model != nil {
// 		if err := decryptModel(q.encryptor, q.model); err != nil {
// 			return err
// 		}
// 	}

// 	// Decrypt additional values
// 	for _, v := range values {
// 		if err := decryptModel(q.encryptor, v); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// // SelectAndCount selects and counts with decryption
// func (q *Query) SelectAndCount(values ...interface{}) (count int, firstErr error) {
// 	count, err := q.Query.SelectAndCount(values...)
// 	if err != nil {
// 		return count, err
// 	}

// 	// Decrypt model if exists
// 	if q.model != nil {
// 		if err := decryptModel(q.encryptor, q.model); err != nil {
// 			return count, err
// 		}
// 	}

// 	// Decrypt additional values
// 	for _, v := range values {
// 		if err := decryptModel(q.encryptor, v); err != nil {
// 			return count, err
// 		}
// 	}

// 	return count, nil
// }

// // First selects the first row with decryption
// func (q *Query) First() error {
// 	err := q.Query.First()
// 	if err != nil {
// 		return err
// 	}

// 	// Decrypt model if exists
// 	if q.model != nil {
// 		return decryptModel(q.encryptor, q.model)
// 	}

// 	return nil
// }

// // Last selects the last row with decryption
// func (q *Query) Last() error {
// 	err := q.Query.Last()
// 	if err != nil {
// 		return err
// 	}

// 	// Decrypt model if exists
// 	if q.model != nil {
// 		return decryptModel(q.encryptor, q.model)
// 	}

// 	return nil
// }

// // Delete deletes the model (no encryption needed)
// func (q *Query) Delete(values ...interface{}) (orm.Result, error) {
// 	return q.Query.Delete(values...)
// }

// // encryptModel encrypts fields tagged with encrypted:"true"
// func encryptModel(encryptor *govault.Encryptor, model interface{}) error {
// 	if model == nil {
// 		return nil
// 	}

// 	val := reflect.ValueOf(model)
// 	if val.Kind() == reflect.Ptr {
// 		val = val.Elem()
// 	}

// 	// Handle slice of models
// 	if val.Kind() == reflect.Slice {
// 		for i := 0; i < val.Len(); i++ {
// 			elem := val.Index(i)
// 			if elem.Kind() == reflect.Ptr {
// 				if err := encryptModel(encryptor, elem.Interface()); err != nil {
// 					return err
// 				}
// 			} else {
// 				if elem.CanAddr() {
// 					if err := encryptModel(encryptor, elem.Addr().Interface()); err != nil {
// 						return err
// 					}
// 				}
// 			}
// 		}
// 		return nil
// 	}

// 	if val.Kind() != reflect.Struct {
// 		return nil
// 	}

// 	typ := val.Type()
// 	for i := 0; i < val.NumField(); i++ {
// 		field := val.Field(i)
// 		fieldType := typ.Field(i)

// 		// Check for encrypted tag
// 		if fieldType.Tag.Get("encrypted") == "true" {
// 			if field.Kind() == reflect.String && field.CanSet() {
// 				plaintext := field.String()
// 				if plaintext != "" {
// 					encrypted, err := encryptor.Encrypt(plaintext)
// 					if err != nil {
// 						return err
// 					}
// 					field.SetString(encrypted)
// 				}
// 			}
// 		}
// 	}

// 	return nil
// }

// // decryptModel decrypts fields tagged with encrypted:"true"
// func decryptModel(encryptor *govault.Encryptor, model interface{}) error {
// 	if model == nil {
// 		return nil
// 	}

// 	val := reflect.ValueOf(model)
// 	if val.Kind() == reflect.Ptr {
// 		val = val.Elem()
// 	}

// 	// Handle slice of models
// 	if val.Kind() == reflect.Slice {
// 		for i := 0; i < val.Len(); i++ {
// 			elem := val.Index(i)
// 			if elem.Kind() == reflect.Ptr {
// 				if err := decryptModel(encryptor, elem.Interface()); err != nil {
// 					return err
// 				}
// 			} else {
// 				if elem.CanAddr() {
// 					if err := decryptModel(encryptor, elem.Addr().Interface()); err != nil {
// 						return err
// 					}
// 				}
// 			}
// 		}
// 		return nil
// 	}

// 	if val.Kind() != reflect.Struct {
// 		return nil
// 	}

// 	typ := val.Type()
// 	for i := 0; i < val.NumField(); i++ {
// 		field := val.Field(i)
// 		fieldType := typ.Field(i)

// 		if fieldType.Tag.Get("encrypted") == "true" {
// 			if field.Kind() == reflect.String && field.CanSet() {
// 				ciphertext := field.String()
// 				if ciphertext != "" {
// 					decrypted, err := encryptor.Decrypt(ciphertext)
// 					if err != nil {
// 						return err
// 					}
// 					field.SetString(decrypted)
// 				}
// 			}
// 		}
// 	}

// 	return nil
// }
