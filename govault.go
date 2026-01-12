package govault

import (
	"fmt"

	"github.com/muhammadluth/govault/internal"

	gb "github.com/muhammadluth/govault/bun"
)

// Re-export types from internal
type AdapterName = internal.AdapterName
type Config = internal.Config

const (
	AdapterNameBun  = internal.AdapterNameBun
	AdapterNameGoPg = internal.AdapterNameGoPg
)

// GovaultDB is now a wrapper struct embedding the internal type
type GovaultDB struct {
	*internal.GovaultDB
}

// New creates a new govault DB and returns the wrapper
func New(config Config) (*GovaultDB, error) {
	internalGovault, err := internal.New(config)
	if err != nil {
		return nil, err
	}

	adapter, err := detectAdapter(config, internalGovault)
	if err != nil {
		return nil, err
	}

	internalGovault.DB = adapter
	return &GovaultDB{internalGovault}, nil // Return the wrapper
}

// detectAdapter detects which ORM adapter to use
func detectAdapter(config Config, govault *internal.GovaultDB) (any, error) {
	switch config.AdapterName {
	case AdapterNameBun:
		if config.BunDB == nil {
			return nil, fmt.Errorf("BunDB is nil")
		}
		db := gb.BunWrapQueries(config.BunDB, govault)
		return db, nil
	case AdapterNameGoPg:
		if config.GoPgDB == nil {
			return nil, fmt.Errorf("GoPgDB is nil")
		}
		// Assuming GoPgWrapQueries is implemented elsewhere (e.g., in a separate file)
		// db := GoPgWrapQueries(config.GoPgDB, govault)
		// return db, nil
	}
	return nil, fmt.Errorf("unsupported ORM: %s", config.AdapterName)
}

// BunDB returns the underlying Bun database
func (g *GovaultDB) BunDB() *gb.BunDB {
	if bunDB, ok := g.DB.(*gb.BunDB); ok {
		return bunDB
	}
	return nil
}

// // GoPgDB returns the underlying go-pg database
// func (g *GovaultDB) GoPgDB() *GoPgDB {
// 	if goPgDB, ok := g.DB.(*GoPgDB); ok {
// 		return goPgDB
// 	}
// 	return nil
// }
