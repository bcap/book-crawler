package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"time"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/crawler"
	"github.com/bcap/book-crawler/dot"
	"github.com/bcap/book-crawler/log"
	"github.com/bcap/book-crawler/storage/neo4j"

	"github.com/spf13/cobra"
)

var maxDepth int
var maxReadAlso int
var minNumRatings int32
var maxNumRatings int32
var minRating int32
var maxRating int32
var maxParallelism int
var maxRequestRetries int
var minRequestRetryWait time.Duration
var maxRequestRetryWait time.Duration
var printDot bool
var useNeo4J bool
var neo4JURL string
var neo4JUser string
var neo4JPassword string
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
	cmd.Flags().Int32Var(&minNumRatings, "min-num-ratings", -1, "only persist and follow links for books that have at least this amount of ratings given by users. Set to a negative number to disable this check")
	cmd.Flags().Int32Var(&maxNumRatings, "max-num-ratings", -1, "only persist and follow links for books that have at most this amount of ratings given by users. Set to a negative number to disable this check")
	cmd.Flags().Int32Var(&minRating, "min-rating", -1, "only persist and follow links for books that have at least this rating. Set to a negative number to disable this check")
	cmd.Flags().Int32Var(&maxRating, "max-rating", -1, "only persist and follow links for books that have at most this rating. Set to a negative number to disable this check")
	cmd.Flags().IntVarP(&maxParallelism, "parallelism", "p", 10, "controls how requests are alowed in parallel")
	cmd.Flags().IntVar(&maxRequestRetries, "max-retries", 4, "controls how many times the crawler will retry for a given URL")
	cmd.Flags().DurationVar(&minRequestRetryWait, "min-retry-wait", 1*time.Second, "minimum time to wait in between retries")
	cmd.Flags().DurationVar(&maxRequestRetryWait, "max-retry-wait", 15*time.Second, "maximum time to wait in between retries")
	cmd.Flags().BoolVar(&printDot, "dot", false, "print the run results as a dot file (stdout)")
	cmd.Flags().BoolVar(&useNeo4J, "neo4j", false, "use neo4j as storage")
	cmd.Flags().StringVar(&neo4JURL, "neo4j-url", neo4j.DefaultURL, "neo4j database address")
	cmd.Flags().StringVar(&neo4JUser, "neo4j-user", "", "user when connecting to the neo4j database")
	cmd.Flags().StringVar(&neo4JPassword, "neo4j-password", "", "password when connecting to the neo4j database")
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
		crawler.WithMinNumRatings(minNumRatings),
		crawler.WithMinRating(minRating),
		crawler.WithMinRating(maxRating),
		crawler.WithMaxParallelism(maxParallelism),
		crawler.WithRequestMaxRetries(maxRequestRetries),
		crawler.WithRequestMinRetryWait(minRequestRetryWait),
		crawler.WithRequestMaxRetryWait(maxRequestRetryWait),
	)

	if useNeo4J {
		storage := neo4j.New(neo4JURL)
		storage.User = neo4JUser
		storage.Password = neo4JPassword
		if err := storage.Initialize(cmd.Context()); err != nil {
			panic(err)
		}
		defer storage.Shutdown(cmd.Context())
		crawler.Storage = storage
	}

	if err := crawler.Storage.Initialize(cmd.Context()); err != nil {
		panic(err)
	}

	url := args[0]

	err := crawler.Crawl(cmd.Context(), url)
	if err != nil {
		panic(err)
	}

	rootBook, err := crawler.Storage.GetBook(cmd.Context(), url, 0)
	if err != nil {
		panic(err)
	}

	if printDot {
		log.Infof("printing results as a dot file")
		graph := book.NewGraph(rootBook)
		if err != nil {
			panic(err)
		}
		dot.PrintBookGraph(graph, os.Stdout)
	}

	if err := crawler.Storage.Shutdown(cmd.Context()); err != nil {
		panic(err)
	}
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
