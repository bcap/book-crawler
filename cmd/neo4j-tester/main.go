package main

import (
	"context"
	"fmt"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/log"
	"github.com/bcap/book-crawler/storage"
	"github.com/bcap/book-crawler/storage/neo4j"
	"github.com/davecgh/go-spew/spew"
)

func main() {
	log.Level = log.DebugLevel
	ctx := context.Background()
	s := neo4j.New(neo4j.DefaultURL)
	b1 := book.Book{
		Title:        "test title 1",
		Author:       "test author 1",
		AuthorURL:    "http://testauthor1",
		Rating:       4.1,
		RatingsTotal: 1000,
		Reviews:      2000,
		URL:          "http://test1",
		AlsoRead:     []book.Edge{},
	}
	b2 := book.Book{
		Title:        "test title 2",
		Author:       "test author 1",
		AuthorURL:    "http://testauthor1",
		Rating:       3.2,
		RatingsTotal: 3000,
		Reviews:      4000,
		URL:          "http://test2",
		AlsoRead:     []book.Edge{},
	}
	b3 := book.Book{
		Title:        "test title 3",
		Author:       "test author 2",
		AuthorURL:    "http://testauthor2",
		Rating:       3.2,
		RatingsTotal: 3000,
		Reviews:      4000,
		URL:          "http://test3",
		AlsoRead:     []book.Edge{},
	}

	fmt.Println(spew.Sdump(s.Initialize(ctx)))
	state := storage.StateChange{}
	fmt.Println(spew.Sdump(s.SetBookState(ctx, b1.URL, state, storage.BeingCrawled)))
	state, err := s.GetBookState(ctx, b1.URL)
	fmt.Println(spew.Sdump(state, err))
	fmt.Println(spew.Sdump(s.SetBookState(ctx, b1.URL, state, storage.Crawled)))
	fmt.Println(spew.Sdump(s.SetBook(ctx, b1.URL, &b1)))
	fmt.Println(spew.Sdump(s.SetBook(ctx, b2.URL, &b2)))
	fmt.Println(spew.Sdump(s.SetBook(ctx, b3.URL, &b3)))
	fmt.Println(spew.Sdump(s.LinkBook(ctx, b1.URL, b2.URL, 0)))
	fmt.Println(spew.Sdump(s.LinkBook(ctx, b2.URL, b3.URL, 0)))
	fmt.Println(spew.Sdump(s.GetBookState(ctx, b1.URL)))
	fmt.Println(spew.Sdump(s.GetBook(ctx, b2.URL, 3)))
	fmt.Println(spew.Sdump(s.Shutdown(ctx)))
}
