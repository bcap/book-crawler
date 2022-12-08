package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/storage"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

const DefaultURL = "neo4j://localhost:7687"

var initStatements = []string{
	"CREATE CONSTRAINT IF NOT EXISTS FOR (b:Book) REQUIRE (b.url) IS UNIQUE",
	"CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.title)",
	"CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.author)",
}

type Storage struct {
	URL         string
	User        string
	Password    string
	BearerToken string

	driver neo4j.DriverWithContext
}

func New(url string) *Storage {
	return &Storage{
		URL: url,
	}
}

func (s *Storage) Initialize(ctx context.Context) error {
	var auth neo4j.AuthToken
	if s.User != "" {
		auth = neo4j.BasicAuth(s.User, s.Password, "")
	} else if s.BearerToken != "" {
		auth = neo4j.BearerAuth(s.BearerToken)
	} else {
		auth = neo4j.NoAuth()
	}
	driver, err := neo4j.NewDriverWithContext(s.URL, auth)
	if err != nil {
		return fmt.Errorf("failed to create neo4j driver: %w", err)
	}
	s.driver = driver

	return s.runInitStatements(ctx)
}

func (s *Storage) Shutdown(ctx context.Context) error {
	return s.driver.Close(ctx)
}

func (s *Storage) GetBookState(ctx context.Context, url string) (storage.State, error) {
	work := func(tx neo4j.ManagedTransaction) (storage.State, error) {
		query := "MATCH (b:Book {url: $url}) RETURN b.state"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return storage.NotCrawled, NewErrQuery(query, err)
		}
		if records.Next(ctx) {
			value := records.Record().Values[0]
			return storage.State(value.(int64)), nil
		}
		return storage.NotCrawled, nil
	}
	result, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, 0)
	return result.(storage.State), err
}

func (s *Storage) SetBookState(ctx context.Context, url string, previous storage.State, new storage.State) (bool, error) {
	work := func(tx neo4j.ManagedTransaction) (bool, error) {
		query := "MATCH (b:Book {url: $url}) RETURN id(b)"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return false, NewErrQuery(query, err)
		}
		if !records.Peek(ctx) {
			if previous != storage.NotCrawled {
				return false, nil
			}
			query = "CREATE (b:Book {url: $url, state: $state})"
			if _, err := tx.Run(ctx, query, map[string]any{"url": url, "state": new}); err != nil {
				return false, NewErrQuery(query, err)
			}
			return true, nil
		}

		query = "" +
			"MATCH (b:Book {url: $url, state: $oldState}) " +
			"SET b.state = $newState " +
			"RETURN b "
		records, err = tx.Run(
			ctx, query,
			map[string]any{"url": url, "oldState": previous, "newState": new},
		)
		if err != nil {
			return false, NewErrQuery(query, err)
		}

		return records.Peek(ctx), nil
	}
	result, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, false)
	return result.(bool), err
}

func (s *Storage) GetBook(ctx context.Context, url string, maxDepth int) (*book.Book, error) {
	work := func(tx neo4j.ManagedTransaction) (*book.Book, error) {
		query := "MATCH (b:Book {url: $url}) RETURN b"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return nil, NewErrQuery(query, err)
		}
		if records.Next(ctx) {
			node := records.Record().Values[0].(dbtype.Node)
			return book.FromNeo4jNode(&node), nil
		}
		return nil, nil
	}
	result, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, nil)
	return result.(*book.Book), err
}

func (s *Storage) SetBook(ctx context.Context, url string, book *book.Book) error {
	work := func(tx neo4j.ManagedTransaction) (int64, error) {
		attrs := book.ToNeo4jAttributes()
		query := fmt.Sprintf("MERGE (b:Book {url: $url}) SET %s RETURN id(b)", toSetString("b", attrs))
		records, err := tx.Run(ctx, query, attrs)
		if err != nil {
			return 0, NewErrQuery(query, err)
		}
		records.Next(ctx)
		return records.Record().Values[0].(int64), nil
	}
	_, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, 0)
	return err
}

func (s *Storage) LinkBooks(ctx context.Context, url string, bookURLs ...string) error {
	work := func(tx neo4j.ManagedTransaction) (struct{}, error) {
		query := "" +
			"MATCH (b:Book {url: $b_url}), (o:Book {url: $o_url}) " +
			"MERGE (b)-[r:ALSO_READ {idx: $idx}]->(o) "
		for idx, relatedURL := range bookURLs {
			params := map[string]any{"b_url": url, "o_url": relatedURL, "idx": idx}
			_, err := tx.Run(ctx, query, params)
			if err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	}
	_, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, struct{}{})
	return err
}

func (s *Storage) runInitStatements(ctx context.Context) error {
	_, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) {
		for _, stmt := range initStatements {
			if _, err := tx.Run(ctx, stmt, nil); err != nil {
				return nil, NewErrQuery(stmt, err)
			}
		}
		return nil, nil
	}, nil)
	return err
}

func (s *Storage) execute(
	ctx context.Context,
	write bool,
	work neo4j.ManagedTransactionWork,
	zeroV any,
	configurers ...func(*neo4j.TransactionConfig),
) (any, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	function := session.ExecuteRead
	if write {
		function = session.ExecuteWrite
	}
	result, err := function(ctx, work, configurers...)
	if err != nil {
		return zeroV, err
	}
	return result, nil
}

func toSetString(alias string, params map[string]any) string {
	b := strings.Builder{}
	i := 0
	for key := range params {
		b.WriteString(alias)
		b.WriteString(".")
		b.WriteString(key)
		b.WriteString(" = $")
		b.WriteString(key)
		if i != len(params)-1 {
			b.WriteString(", ")
		}
		i++
	}
	return b.String()
}

// Making sure Storage implements Storage
var _ storage.Storage = &Storage{}
