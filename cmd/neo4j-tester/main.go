package main

import (
	"context"
	"fmt"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/storage"
	"github.com/bcap/book-crawler/storage/neo4j"
)

func main() {
	ctx := context.Background()
	s := neo4j.New(neo4j.DefaultURL)
	b1 := book.Book{
		Title:        "test title 1",
		Author:       "test author 1",
		Rating:       4.1,
		RatingsTotal: 1000,
		Reviews:      2000,
		URL:          "http://test1",
		AlsoRead:     []*book.Book{},
	}
	b2 := book.Book{
		Title:        "test title 2",
		Author:       "test author 2",
		Rating:       3.2,
		RatingsTotal: 3000,
		Reviews:      4000,
		URL:          "http://test2",
		AlsoRead:     []*book.Book{},
	}

	fmt.Println(s.Initialize(ctx))
	fmt.Println(s.SetBookState(ctx, b1.URL, storage.NotCrawled, storage.BeingCrawled))
	fmt.Println(s.GetBookState(ctx, b1.URL))
	fmt.Println(s.SetBookState(ctx, b1.URL, storage.BeingCrawled, storage.Crawled))
	fmt.Println(s.SetBook(ctx, b1.URL, &b1))
	fmt.Println(s.SetBook(ctx, b2.URL, &b2))
	fmt.Println(s.GetBookState(ctx, b1.URL))
	fmt.Println(s.GetBook(ctx, b1.URL, 0))
	fmt.Println(s.GetBook(ctx, b2.URL, 0))
	fmt.Println(s.LinkBooks(ctx, b1.URL, b2.URL))
	fmt.Println(s.Shutdown(ctx))
}
