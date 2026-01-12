package bun_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/muhammadluth/govault"
	gb "github.com/muhammadluth/govault/bun"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

type TestUser struct {
	bun.BaseModel `bun:"table:test_users,alias:u"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Email         string `bun:"email,notnull" encrypted:"true"`
	Phone         string `bun:"phone" encrypted:"true"`
	Address       string `bun:"address"`
}

type TestUserWithProfile struct {
	bun.BaseModel `bun:"table:test_users,alias:u"`
	ID            int64        `bun:"id,pk,autoincrement"`
	Name          string       `bun:"name,notnull"`
	Email         string       `bun:"email,notnull" encrypted:"true"`
	Phone         string       `bun:"phone" encrypted:"true"`
	Address       string       `bun:"address"`
	Profile       *TestProfile `bun:"rel:has-one,join:id=user_id"`
}

type TestProfile struct {
	bun.BaseModel `bun:"table:test_profiles,alias:p"`
	ID            int64     `bun:"id,pk,autoincrement"`
	UserID        int64     `bun:"user_id"`
	Bio           string    `bun:"bio" encrypted:"true"`
	User          *TestUser `bun:"rel:belongs-to,join:user_id=id"`
}

type TestUserWithInt struct {
	bun.BaseModel `bun:"table:test_users_int"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	Age           int    `bun:"age" encrypted:"true"` // Int field with encrypted tag
	Email         string `bun:"email,notnull" encrypted:"true"`
}

type TestUserWithPrivate struct {
	bun.BaseModel `bun:"table:test_users_private"`
	ID            int64  `bun:"id,pk,autoincrement"`
	Name          string `bun:"name,notnull"`
	email         string `bun:"email" encrypted:"true"` // private field
	Email         string `bun:"email_public"`
}

func setupTestDB(t *testing.T) (*gb.BunDB, *govault.GovaultDB, func()) {
	// Setup Bun connection
	openDB := sql.OpenDB(pgdriver.NewConnector(
		pgdriver.WithNetwork("tcp"),
		pgdriver.WithAddr("localhost:5433"),
		pgdriver.WithUser("postgres"),
		pgdriver.WithPassword("Admin123!"),
		pgdriver.WithDatabase("postgres"),
		pgdriver.WithApplicationName("playground"),
		pgdriver.WithTLSConfig(nil),
		pgdriver.WithDialTimeout(5*time.Second),
	))

	bunDB := bun.NewDB(openDB, pgdialect.New())

	goVaultDB, err := govault.New(govault.Config{
		AdapterName: govault.AdapterNameBun,
		BunDB:       bunDB,
		Keys: map[string][]byte{
			"1": []byte("727d37a0-a5f2-4d67-af47-83039c8e"),
			"2": []byte("e778dc27-9b04-44c3-a862-feba061c"),
			"3": []byte("e778dc27-9b04-44c3-a862-83039c8e"),
		},
		DefaultKeyID: "3", // Key 3 is default for encryption
	})
	if err != nil {
		panic(err)
	}
	db := goVaultDB.BunDB()

	// Create table
	ctx := context.Background()
	_, err = db.NewCreateTable().
		Model((*TestUser)(nil)).
		IfNotExists().
		Exec(ctx)
	require.NoError(t, err)

	// Clean table
	_, err = db.NewDelete().Model((*TestUser)(nil)).Where("1=1").Exec(ctx)
	require.NoError(t, err)

	cleanup := func() {
		db.NewDropTable().Model((*TestUser)(nil)).IfExists().Exec(ctx)
		openDB.Close()
	}

	return db, goVaultDB, cleanup
}
