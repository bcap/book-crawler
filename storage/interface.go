package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/bcap/book-crawler/book"
)

type State int32

const (
	NotCrawled   State = 0
	BeingCrawled State = 1
	Crawled      State = 2
	Linked       State = 3
)

type StateChange struct {
	When  time.Time
	State State
}

type url = string

type Storage interface {
	Initialize(ctx context.Context) error
	Shutdown(ctx context.Context) error

	// State manipulation is a CAS operation (Compare And Swap)
	GetBookState(ctx context.Context, url url) (StateChange, error)
	SetBookState(ctx context.Context, url url, previous StateChange, new State) (StateChange, bool, error)

	GetBook(ctx context.Context, url url, maxDepth int) (*book.Book, error)
	SetBook(ctx context.Context, url url, book *book.Book) error
	LinkBooks(ctx context.Context, url url, bookUrls ...url) error
}

type ErrBookNotFound struct {
	URL string
}

func (e ErrBookNotFound) Error() string {
	if e.URL != "" {
		return fmt.Sprintf("book not found: %s", e.URL)
	}
	return "book not found"
}
