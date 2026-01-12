// Package govault - Bun adapter select query implementation
package bun

import (
	"context"
	"fmt"

	"github.com/muhammadluth/govault/internal"
	"github.com/uptrace/bun"
)

// BunSelectQuery wraps bun.SelectQuery
type BunSelectQuery struct {
	*bun.SelectQuery
	govault *internal.GovaultDB
}

// Conn sets the database connection
func (q *BunSelectQuery) Conn(db bun.IConn) *BunSelectQuery {
	q.SelectQuery.Conn(db)
	return q
}

// Model sets the model for select
func (q *BunSelectQuery) Model(model any) *BunSelectQuery {
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
		q.Err(fmt.Errorf("only one apply function is supported"))
		return q
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

// Scan executes the query and decrypts results
func (q *BunSelectQuery) Scan(ctx context.Context, dest ...any) error {
	err := q.SelectQuery.Scan(ctx, dest...)
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

// ScanAndCount scans results and returns count
func (q *BunSelectQuery) ScanAndCount(ctx context.Context, dest ...any) (int, error) {
	count, err := q.SelectQuery.ScanAndCount(ctx, dest...)
	if err != nil {
		return count, err
	}

	for _, d := range dest {
		if err := q.govault.DecryptRecursive(d); err != nil {
			return count, err
		}
	}

	return count, nil
}
