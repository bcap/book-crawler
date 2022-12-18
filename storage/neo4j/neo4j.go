package neo4j

import (
	"context"
	"fmt"
	"math/rand"
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
		var query string
		var params map[string]any
		when := time.Now().UTC()
		if previous.State == 0 {
			query = "" +
				"MERGE (b:Book {url: $url}) " +
				"WITH b, b.crawlStateChanged as previousWhen, b.crawlState as previousState " +
				"WHERE (previousState = 0 AND previousWhen = $previousWhen) " +
				"OR (previousState is null AND previousWhen is null) " +
				"SET b.crawlState = $newState, b.crawlStateChanged = $newWhen " +
				"RETURN previousWhen, previousState "
			params = map[string]any{
				"url":          url,
				"previousWhen": previous.When,
				"newState":     new,
				"newWhen":      when,
			}
		} else {
			query = "" +
				"MERGE (b:Book {url: $url}) " +
				"WITH b, b.crawlStateChanged as previousWhen, b.crawlState as previousState " +
				"WHERE previousState = $previousState " +
				"AND previousWhen = $previousWhen " +
				"SET b.crawlState = $newState, b.crawlStateChanged = $newWhen " +
				"RETURN previousWhen, previousState "
			params = map[string]any{
				"url":           url,
				"previousState": previous.State,
				"previousWhen":  previous.When,
				"newState":      new,
				"newWhen":       when,
			}
		}

		records, err := tx.Run(ctx, query, params)
		if err != nil {
			return storage.StateChange{}, NewErrQuery(query, err)
		}

		// CAS check failed, not changed
		if !records.Peek(ctx) {
			return storage.StateChange{}, nil
		}

		// CAS suceeded, changed
		return storage.StateChange{
			State: new,
			When:  when,
		}, nil
	}
	result, err := execute(ctx, s.driver, true, work)
	if err != nil {
		return storage.StateChange{}, false, err
	}
	zeroV := storage.StateChange{}
	return result, !result.Equals(zeroV), nil
}

func (s *Storage) GetBook(ctx context.Context, url string, maxDepth int) (*book.Book, error) {
	work := func(tx managedTransaction, url string, depth int) (*book.Book, error) {
		log.Debugf("GetBook(url: %v, depth: %v", url, depth)

		query := fmt.Sprintf(""+
			"MATCH (b1:Book {url: $url}) "+
			"MATCH (b2:Book) "+
			"MATCH (p1:Person)-[:AUTHORED]->(b1) "+
			"MATCH (p2:Person)-[:AUTHORED]->(b2) "+
			"MATCH (b1)-[r:ALSO_READ*0..%d]->(b2) "+
			"RETURN b2, p2, r ",
			maxDepth,
		)

		records, err := tx.Run(ctx, query, map[string]any{"url": url})
		if err != nil {
			return nil, NewErrQuery(query, err)
		}
		if !records.Next(ctx) {
			return nil, nil
		}

		idMap := map[string]*book.Book{}

		value := func(node *dbtype.Node, key string, defaultValue any) any {
			if v, has := node.Props[key]; has {
				return v
			}
			return defaultValue
		}

		var rootBook *book.Book

		for {
			bookNode := records.Record().Values[0].(dbtype.Node)
			authorNode := records.Record().Values[1].(dbtype.Node)
			relationships := records.Record().Values[2].([]interface{})

			if _, has := idMap[bookNode.ElementId]; !has {
				b := &book.Book{
					Title:        value(&bookNode, "title", "").(string),
					Rating:       float32(value(&bookNode, "rating", 0.0).(float64)),
					RatingsTotal: int32(value(&bookNode, "ratings", 0).(int64)),
					Reviews:      int32(value(&bookNode, "reviews", 0).(int64)),
					URL:          value(&bookNode, "url", "").(string),
					Author:       value(&authorNode, "name", "").(string),
					AuthorURL:    value(&authorNode, "url", "").(string),
					AlsoRead:     []book.Edge{},
				}
				idMap[bookNode.ElementId] = b
			}

			if len(relationships) == 0 {
				rootBook = idMap[bookNode.ElementId]
			} else {
				lastRelationship := relationships[len(relationships)-1].(dbtype.Relationship)
				from := idMap[lastRelationship.StartElementId]
				to := idMap[lastRelationship.EndElementId]
				priority := 0
				priorityIntf, hasPriority := lastRelationship.Props["priority"]
				if hasPriority {
					priority = int(priorityIntf.(int64))
				}
				from.AlsoRead = append(from.AlsoRead, book.Edge{
					From:     from,
					To:       to,
					Priority: priority,
				})
			}

			if !records.Next(ctx) {
				break
			}
		}

		return rootBook, nil
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
