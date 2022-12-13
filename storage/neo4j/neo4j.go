package neo4j

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/log"
	"github.com/bcap/book-crawler/storage"
	"github.com/davecgh/go-spew/spew"

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
	work := func(tx managedTransaction) (storage.StateChange, error) {
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
	return execute(ctx, s.driver, true, work)
}

func (s *Storage) SetBookState(ctx context.Context, url string, previous storage.StateChange, new storage.State) (storage.StateChange, bool, error) {
	work := func(tx managedTransaction) (storage.StateChange, error) {
		query := "MATCH (b:Book {url: $url}) RETURN id(b)"
		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		when := time.Now().UTC()
		if err != nil {
			return storage.StateChange{}, NewErrQuery(query, err)
		}
		if !records.Peek(ctx) {
			query = "CREATE (b:Book {url: $url, crawlState: $state, crawlStateChanged: $when})"
			params := map[string]any{"url": url, "state": new, "when": when}
			if _, err := tx.Run(ctx, query, params); err != nil {
				return storage.StateChange{}, NewErrQuery(query, err)
			}
			return storage.StateChange{
				State: new,
				When:  when,
			}, nil
		}

		query = "" +
			"MATCH (b:Book {url: $url, crawlState: $oldState}) " +
			"SET b.crawlState = $newState, b.crawlStateChanged = $when " +
			"RETURN id(b)"
		params := map[string]any{
			"url":      url,
			"oldState": previous.State,
			"newState": new,
			"when":     when,
		}
		records, err = tx.Run(ctx, query, params)
		if err != nil {
			return storage.StateChange{}, NewErrQuery(query, err)
		}

		if records.Peek(ctx) {
			return storage.StateChange{
				State: new,
				When:  when,
			}, nil
		}
		return storage.StateChange{}, nil
	}
	sc, err := execute(ctx, s.driver, true, work)
	if err != nil {
		return sc, false, err
	}
	zeroV := storage.StateChange{}
	return sc, !reflect.DeepEqual(sc, zeroV), nil
}

func (s *Storage) GetBook(ctx context.Context, url string, maxDepth int) (*book.Book, error) {
	var work func(tx managedTransaction, url string, depth int) (*book.Book, error)
	work = func(tx managedTransaction, url string, depth int) (*book.Book, error) {
		log.Debugf("GetBook(url: %v, depth: %v", url, depth)
		var query string
		if depth < maxDepth {
			query = "" +
				"MATCH (p:Person)-[:AUTHORED]->(b1:Book {url: $url}) " +
				"OPTIONAL MATCH (b1)-[r:ALSO_READ]->(b2:Book) " +
				"RETURN p, b1, r.priority, b2.url " +
				"ORDER BY r.priority ASC "
		} else {
			query = "" +
				"MATCH (p:Person)-[:AUTHORED]->(b:Book {url: $url}) " +
				"RETURN p, b "
		}

		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return nil, NewErrQuery(query, err)
		}
		if !records.Next(ctx) {
			return nil, nil
		}
		personNode := records.Record().Values[0].(dbtype.Node)
		bookNode := records.Record().Values[1].(dbtype.Node)
		value := func(node *dbtype.Node, key string, defaultValue any) any {
			if v, has := node.Props[key]; has {
				return v
			}
			return defaultValue
		}
		b := &book.Book{
			Title:        value(&bookNode, "title", "").(string),
			Rating:       float32(value(&bookNode, "rating", 0.0).(float64)),
			RatingsTotal: int32(value(&bookNode, "ratings", 0).(int64)),
			Reviews:      int32(value(&bookNode, "reviews", 0).(int64)),
			URL:          value(&bookNode, "url", "").(string),
			Author:       value(&personNode, "name", "").(string),
			AuthorURL:    value(&personNode, "url", "").(string),
			AlsoRead:     []book.Edge{},
		}
		if depth < maxDepth {
			for {
				priorityIntf := records.Record().Values[2]
				rURLIntf := records.Record().Values[3]
				if priorityIntf == nil || rURLIntf == nil {
					break
				}
				r, err := work(tx, rURLIntf.(string), depth+1)
				if err != nil {
					return nil, err
				}
				if r == nil {
					continue
				}
				edge := book.Edge{From: b, To: r, Priority: int(priorityIntf.(int64))}
				b.AlsoRead = append(b.AlsoRead, edge)
				if !records.Next(ctx) {
					break
				}
			}
		}
		return b, nil
	}
	return execute(ctx, s.driver, false, func(tx managedTransaction) (*book.Book, error) {
		return work(tx, url, 0)
	})
}

func (s *Storage) SetBook(ctx context.Context, url string, book *book.Book) error {
	work := func(tx managedTransaction) (struct{}, error) {
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
	_, err := execute(ctx, s.driver, true, work)
	return err
}

func (s *Storage) LinkBook(ctx context.Context, url string, relatedURL string, priority int) error {
	work := func(tx managedTransaction) (struct{}, error) {
		query := "" +
			"MATCH (b:Book {url: $b_url}), (o:Book {url: $o_url}) " +
			"MERGE (b)-[r:ALSO_READ {priority: $priority}]->(o) "
		params := map[string]any{"b_url": url, "o_url": relatedURL, "priority": priority}
		_, err := tx.Run(ctx, query, params)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}
	_, err := execute(ctx, s.driver, true, work)
	return err
}

func (s *Storage) runInitStatements(ctx context.Context) error {
	_, err := execute(ctx, s.driver, true, func(tx managedTransaction) (struct{}, error) {
		for _, stmt := range initStatements {
			if _, err := tx.Run(ctx, stmt, nil); err != nil {
				return struct{}{}, NewErrQuery(stmt, err)
			}
		}
		return struct{}{}, nil
	})
	return err
}

func execute[T any](
	ctx context.Context,
	driver neo4j.DriverWithContext,
	write bool,
	work func(managedTransaction) (T, error),
	configurers ...func(*neo4j.TransactionConfig),
) (T, error) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	executeFn := session.ExecuteRead
	if write {
		executeFn = session.ExecuteWrite
	}
	workFn := func(tx neo4j.ManagedTransaction) (any, error) {
		wrappedTx := managedTransaction{ManagedTransaction: tx}
		return work(wrappedTx)
	}
	result, err := executeFn(ctx, workFn, configurers...)
	if result == nil {
		var zeroV T
		return zeroV, err
	}
	return result.(T), err
}

type managedTransaction struct {
	neo4j.ManagedTransaction
}

func (t managedTransaction) Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	// generate a 6 digit query id to help logging
	queryId := 100000 + rand.Int31n(900000)
	log.Debugf("running neo4j query %v: %q, params: %v", queryId, cypher, spew.Sprintf("%+#v", params))
	start := time.Now()
	result, err := t.ManagedTransaction.Run(ctx, cypher, params)
	took := time.Since(start)
	if err != nil {
		log.Warnf("neo4j query %v failed in %v: %v", queryId, took, err)
	} else {
		log.Debugf("neo4j query %v executed in %v", queryId, took)
	}
	return result, err
}

// Making sure Storage implements Storage
var _ storage.Storage = &Storage{}
