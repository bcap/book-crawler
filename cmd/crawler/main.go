package main

import (
	"context"
	"os"

	"github.com/bcap/book-crawler/crawler"
	"github.com/bcap/book-crawler/dot"
	"github.com/bcap/book-crawler/log"
)

// Stephen Hawking - A Brief History of Time
const rootBook = "https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time"

func main() {
	log.Level = log.InfoLevel

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	crawler := crawler.NewCrawler(
		crawler.WithMaxDepth(10),
		crawler.WithMaxReadAlso(5),
		crawler.WithMaxParallelism(5),
	)
	bookGraph, err := crawler.Crawl(ctx, rootBook)
	if err != nil {
		panic(err)
	}

	dot.PrintBookGraph(bookGraph, os.Stdout)
}
