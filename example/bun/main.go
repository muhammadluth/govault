package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/muhammadluth/govault"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	gb "github.com/muhammadluth/govault/bun"
)

// User model with encrypted fields
type User struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	ID      int64  `bun:"id,pk,autoincrement"`
	Name    string `bun:"name,notnull"`
	Email   string `bun:"email,notnull" encrypted:"true"` // This field will be encrypted
	Phone   string `bun:"phone" encrypted:"true"`         // This field will also be encrypted
	Address string `bun:"address"`
}

func main() {
	ctx := context.Background()

	// 1. Setup Bun connection (PostgreSQL)
	dsn := "postgres://postgres:Admin123!@localhost:5433/postgres?sslmode=disable"
	sqlDB := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	bunDB := bun.NewDB(sqlDB, pgdialect.New())

	// 2. Initialize Govault
	// You need to provide at least one encryption key.
	// Keys should be 32 bytes for AES-256.
	gv, err := govault.New(govault.Config{
		AdapterName: govault.AdapterNameBun,
		BunDB:       bunDB,
		Keys: map[string][]byte{
			"key-1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"), // 32 bytes key
			"key-2": []byte("e778dc27-9b04-44c3-a862-feba061c"),
		},
		DefaultKeyID: "key-1",
	})
	if err != nil {
		log.Fatalf("failed to initialize govault: %v", err)
	}

	// Get the Bun-specific adapter from govault
	db := gv.BunDB()

	// 3. Create table (if not exists)
	_, err = db.NewCreateTable().
		Model((*User)(nil)).
		IfNotExists().
		Exec(ctx)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	defer db.NewDropTable().Model((*User)(nil)).IfExists().Exec(ctx)

	// 4. Insert a new user
	// The Email and Phone fields will be automatically encrypted before being sent to the database.
	newUser := &User{
		Name:    "John Doe",
		Email:   "john.doe@example.com",
		Phone:   "+628123456789",
		Address: "123 Main St, Jakarta",
	}

	_, err = db.NewInsert().Model(newUser).Exec(ctx)
	if err != nil {
		log.Fatalf("failed to insert user: %v", err)
	}
	fmt.Printf("Inserted user ID: %d\n", newUser.ID)

	// 5. Select the user
	// The Email and Phone fields will be automatically decrypted when scanned into the struct.
	var retrievedUser User
	err = db.NewSelect().
		Model(&retrievedUser).
		Where("id = ?", newUser.ID).
		Scan(ctx)
	if err != nil {
		log.Fatalf("failed to select user: %v", err)
	}

	fmt.Printf("Retrieved User:\n")
	fmt.Printf("  Name:    %s\n", retrievedUser.Name)
	fmt.Printf("  Email:   %s (decrypted)\n", retrievedUser.Email)
	fmt.Printf("  Phone:   %s (decrypted)\n", retrievedUser.Phone)
	fmt.Printf("  Address: %s\n", retrievedUser.Address)

	// 6. Demonstrate multiple keys (Key Rotation support)
	// You can specify which key to use for a specific operation.
	rotatedUser := &User{
		Name:  "Jane Smith",
		Email: "jane.smith@example.com",
	}
	_, err = db.WithKey("key-2").NewInsert().Model(rotatedUser).Exec(ctx)
	if err != nil {
		log.Fatalf("failed to insert rotated user: %v", err)
	}
	fmt.Printf("\nInserted rotated user ID: %d (using key-2)\n", rotatedUser.ID)

	// 7. Manual Transaction
	err = db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx *gb.BunTx) error {
		txUser := &User{
			Name:  "Tx User",
			Email: "tx@example.com",
		}
		_, err := tx.NewInsert().Model(txUser).Exec(ctx)
		return err
	})
	if err != nil {
		log.Fatalf("transaction failed: %v", err)
	}
	fmt.Println("Transaction completed successfully.")

	// 8. Raw SQL Query
	var email string
	err = db.NewRaw("SELECT email FROM users WHERE id = ?", newUser.ID).Scan(ctx, &email)
	if err != nil {
		log.Fatalf("raw query failed: %v", err)
	}
	fmt.Printf("\nRetrieved email via Raw SQL: %s (automatically decrypted)\n", email)
}
