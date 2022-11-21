package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"time"

	"github.com/bcap/book-crawler/crawler"
	"github.com/bcap/book-crawler/dot"
	"github.com/bcap/book-crawler/log"
	"github.com/spf13/cobra"
)

var maxDepth int
var maxReadAlso int
var maxParallelism int
var maxRequestRetries int
var minRequestRetryWait time.Duration
var maxRequestRetryWait time.Duration
var verbose bool

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := parser()
	cmd.ExecuteContext(ctx)
}

func parser() cobra.Command {
	cmd := cobra.Command{
		Use:  "book-crawler",
		Args: func(cmd *cobra.Command, args []string) error { return validateArgs(args) },
		Run:  run,
	}
	cmd.Flags().IntVarP(&maxDepth, "max-depth", "d", 3, "controls how deep to traverse the graph")
	cmd.Flags().IntVarP(&maxReadAlso, "max-read-also", "r", 5, "controls how many related books to follow from a given book")
	cmd.Flags().IntVarP(&maxParallelism, "parallelism", "p", 10, "controls how requests are alowed in parallel")
	cmd.Flags().IntVar(&maxRequestRetries, "max-retries", 4, "controls how many times the crawler will retry for a given URL")
	cmd.Flags().DurationVar(&minRequestRetryWait, "min-retry-wait", 1*time.Second, "minimum time to wait in between retries")
	cmd.Flags().DurationVar(&maxRequestRetryWait, "max-retry-wait", 15*time.Second, "maximum time to wait in between retries")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "be more verbose by logging in debug mode")

	return cmd
}

func run(cmd *cobra.Command, args []string) {
	log.Level = log.InfoLevel
	if verbose {
		log.Level = log.DebugLevel
	}

	crawler := crawler.NewCrawler(
		crawler.WithMaxDepth(maxDepth),
		crawler.WithMaxReadAlso(maxReadAlso),
		crawler.WithMaxParallelism(maxParallelism),
		crawler.WithRequestMaxRetries(maxRequestRetries),
		crawler.WithRequestMinRetryWait(minRequestRetryWait),
		crawler.WithRequestMaxRetryWait(maxRequestRetryWait),
	)

	bookGraph, err := crawler.Crawl(cmd.Context(), args[0])
	if err != nil {
		panic(err)
	}

	dot.PrintBookGraph(bookGraph, os.Stdout)
}

func validateArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("invalid args: expected a single goodreads book url")
	}
	bookURL := args[0]
	if _, err := url.Parse(bookURL); err != nil {
		return err
	}
	return nil
}
