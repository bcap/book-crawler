package neo4j

import (
	"context"
	"fmt"
	"time"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/storage"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

const DefaultURL = "neo4j://localhost:7687"

var initStatements = []string{
	"CREATE CONSTRAINT IF NOT EXISTS FOR (b:Book) REQUIRE (b.url) IS UNIQUE",
	"CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE (p.url) IS UNIQUE",
	"CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.title)",
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

func (s *Storage) GetBookState(ctx context.Context, url string) (storage.StateChange, error) {
	work := func(tx neo4j.ManagedTransaction) (storage.StateChange, error) {
		query := "MATCH (b:Book {url: $url}) RETURN b.crawlState, b.crawlStateChanged"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return storage.StateChange{}, NewErrQuery(query, err)
		}
		if records.Next(ctx) {
			state := records.Record().Values[0]
			stateChanged := records.Record().Values[1]
			return storage.StateChange{
				When:  stateChanged.(time.Time),
				State: storage.State(state.(int64)),
			}, nil
		}
		return storage.StateChange{}, nil
	}
	result, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, 0)
	return result.(storage.StateChange), err
}

func (s *Storage) SetBookState(ctx context.Context, url string, previous storage.State, new storage.State) (bool, error) {
	work := func(tx neo4j.ManagedTransaction) (bool, error) {
		query := "MATCH (b:Book {url: $url}) RETURN id(b)"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return false, NewErrQuery(query, err)
		}
		if !records.Peek(ctx) {
			query = "CREATE (b:Book {url: $url, crawlState: $state, crawlStateChanged: $when})"
			params := map[string]any{"url": url, "state": new, "when": time.Now().UTC()}
			if _, err := tx.Run(ctx, query, params); err != nil {
				return false, NewErrQuery(query, err)
			}
			return true, nil
		}

		query = "" +
			"MATCH (b:Book {url: $url, crawlState: $oldState}) " +
			"SET b.crawlState = $newState, b.crawlStateChanged = $when " +
			"RETURN b "
		params := map[string]any{
			"url":      url,
			"oldState": previous,
			"newState": new,
			"when":     time.Now().UTC(),
		}
		records, err = tx.Run(ctx, query, params)
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
		query := "MATCH (b:Book {url: $url})<-[:AUTHORED]-(p:Person) RETURN b, p"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return nil, NewErrQuery(query, err)
		}
		if !records.Next(ctx) {
			return nil, nil
		}
		bookNode := records.Record().Values[0].(dbtype.Node)
		personNode := records.Record().Values[1].(dbtype.Node)
		value := func(node *dbtype.Node, key string, defaultValue any) any {
			if v, has := node.Props[key]; has {
				return v
			}
			return defaultValue
		}
		return &book.Book{
			Title:        value(&bookNode, "title", "").(string),
			Rating:       float32(value(&bookNode, "rating", 0.0).(float64)),
			RatingsTotal: int32(value(&bookNode, "ratings", 0).(int64)),
			Reviews:      int32(value(&bookNode, "reviews", 0).(int64)),
			URL:          value(&bookNode, "url", "").(string),
			Author:       value(&personNode, "name", "").(string),
			AuthorURL:    value(&personNode, "url", "").(string),
			AlsoRead:     []*book.Book{},
		}, nil
	}
	result, err := s.execute(ctx, true, func(tx neo4j.ManagedTransaction) (any, error) { return work(tx) }, nil)
	return result.(*book.Book), err
}

func (s *Storage) SetBook(ctx context.Context, url string, book *book.Book) error {
	work := func(tx neo4j.ManagedTransaction) (struct{}, error) {
		query := "" +
			"MERGE (b:Book {url: $bookURL}) " +
			"  SET b.title = $title, b.rating = $rating, b.ratings = $ratings, b.reviews = $reviews " +
			"MERGE (p:Person {url: $personURL}) " +
			"  SET p.name = $author " +
			"MERGE (p)-[:AUTHORED]->(b) "
		attrs := map[string]any{
			"title":     book.Title,
			"author":    book.Author,
			"rating":    book.Rating,
			"ratings":   book.RatingsTotal,
			"reviews":   book.Reviews,
			"bookURL":   book.URL,
			"personURL": book.AuthorURL,
		}
		_, err := tx.Run(ctx, query, attrs)
		if err != nil {
			return struct{}{}, NewErrQuery(query, err)
		}
		return struct{}{}, nil
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

// Making sure Storage implements Storage
var _ storage.Storage = &Storage{}
