package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/storage"
)

type Storage struct {
	books      map[string]*book.Book
	booksMutex sync.RWMutex

	state      map[string]storage.StateChange
	stateMutex sync.RWMutex
}

func (s *Storage) Initialize(context.Context) error {
	s.books = make(map[string]*book.Book)
	s.state = make(map[string]storage.StateChange)
	return nil
}

func (s *Storage) Shutdown(ctx context.Context) error {
	s.books = nil
	s.state = nil
	return nil
}

func (s *Storage) GetBookState(ctx context.Context, url string) (storage.StateChange, error) {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()

	return s.state[url], nil
}

func (s *Storage) SetBookState(ctx context.Context, url string, previous storage.State, new storage.State) (bool, error) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// CAS check
	if s.state[url].State != previous {
		return false, nil
	}

	s.state[url] = storage.StateChange{
		When:  time.Now(),
		State: new,
	}

	return true, nil
}

func (s *Storage) GetBook(ctx context.Context, url string, _ int) (*book.Book, error) {
	s.booksMutex.RLock()
	defer s.booksMutex.RUnlock()

	return s.books[url], nil
}

func (s *Storage) SetBook(ctx context.Context, url string, book *book.Book) error {
	s.booksMutex.Lock()
	defer s.booksMutex.Unlock()

	s.books[url] = book
	return nil
}

func (s *Storage) LinkBooks(ctx context.Context, url string, bookURLs ...string) error {
	s.booksMutex.Lock()
	defer s.booksMutex.Unlock()

	b := s.books[url]
	if b == nil {
		return fmt.Errorf("cannot link books: %w", storage.ErrBookNotFound{URL: url})
	}

	for _, url := range bookURLs {
		related := s.books[url]
		if related != nil {
			b.AlsoRead = append(b.AlsoRead, related)
		}
	}

	return nil
}

// Making sure Storage implements Storage
var _ storage.Storage = &Storage{}
