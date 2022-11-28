package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/bcap/book-crawler/book"
)

type InMemoryStorage struct {
	books      map[string]*book.Book
	booksMutex sync.RWMutex

	state      map[string]State
	stateMutex sync.RWMutex
}

func (s *InMemoryStorage) Initialize(context.Context) error {
	s.books = make(map[string]*book.Book)
	s.state = make(map[string]State)
	return nil
}

func (s *InMemoryStorage) Shutdown(ctx context.Context) error {
	s.books = nil
	s.state = nil
	return nil
}

func (s *InMemoryStorage) GetBookState(ctx context.Context, url string) (State, error) {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()

	return s.state[url], nil
}

func (s *InMemoryStorage) SetBookState(ctx context.Context, url string, previous State, new State) (bool, error) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// CAS check
	if s.state[url] != previous {
		return false, nil
	}

	// no-op case
	if previous == new {
		return true, nil
	}

	if new == 0 {
		delete(s.state, url)
	} else {
		s.state[url] = new
	}

	return true, nil
}

func (s *InMemoryStorage) GetBook(ctx context.Context, url string) (*book.Book, error) {
	s.booksMutex.RLock()
	defer s.booksMutex.RUnlock()

	return s.books[url], nil
}

func (s *InMemoryStorage) SetBook(ctx context.Context, url string, book *book.Book) error {
	s.booksMutex.Lock()
	defer s.booksMutex.Unlock()

	s.books[url] = book
	return nil
}

func (s *InMemoryStorage) LinkBooks(ctx context.Context, url string, bookURLs ...url) error {
	s.booksMutex.Lock()
	defer s.booksMutex.Unlock()

	b := s.books[url]
	if b == nil {
		return fmt.Errorf("cannot link books: %w", ErrBookNotFound{URL: url})
	}

	for _, url := range bookURLs {
		related := s.books[url]
		if related != nil {
			b.AlsoRead = append(b.AlsoRead, related)
		}
	}

	return nil
}

func (s *InMemoryStorage) BookGraph(ctx context.Context, root *book.Book, maxDepth int) (book.Graph, error) {
	s.booksMutex.RLock()
	defer s.booksMutex.RUnlock()

	return book.Graph{
		Root:    root,
		All:     book.Collect(root),
		ByDepth: book.CollectByDepth(root),
	}, nil
}

// Making sure InMemoryStorage implements Storage
var _ Storage = &InMemoryStorage{}
