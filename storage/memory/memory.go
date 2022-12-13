package memory

import (
	"context"
	"fmt"
	"sort"
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

func (s *Storage) SetBookState(ctx context.Context, url string, previous storage.StateChange, new storage.State) (storage.StateChange, bool, error) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	// CAS check
	previousSC := s.state[url]
	if previous.State != previousSC.State || previous.When != previousSC.When {
		return storage.StateChange{}, false, nil
	}

	newSC := storage.StateChange{
		When:  time.Now(),
		State: new,
	}
	s.state[url] = newSC
	return newSC, true, nil
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

func (s *Storage) LinkBook(ctx context.Context, url string, relatedURL string, priority int) error {
	s.booksMutex.Lock()
	defer s.booksMutex.Unlock()

	b := s.books[url]
	if b == nil {
		return fmt.Errorf("cannot link books: %w", storage.ErrBookNotFound{URL: url})
	}

	related := s.books[relatedURL]
	if related == nil {
		return nil
	}

	edge := book.Edge{From: b, To: related, Priority: priority}
	b.AlsoRead = append(b.AlsoRead, edge)
	lessFn := func(i, j int) bool {
		return b.AlsoRead[i].Priority < b.AlsoRead[j].Priority
	}
	sort.Slice(b.AlsoRead, lessFn)

	return nil
}

// Making sure Storage implements Storage
var _ storage.Storage = &Storage{}
