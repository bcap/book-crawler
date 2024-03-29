package crawler

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	myhttp "github.com/bcap/book-crawler/http"
	"github.com/bcap/book-crawler/storage"
	"github.com/bcap/book-crawler/storage/memory"
)

var extraStatusCodesToRetry = []int{
	403, // sometimes goodreads returns 403 (Forbidden), but we should retry on it
}

type Crawler struct {
	Client  *myhttp.Client
	Storage storage.Storage

	maxDepth    int
	maxReadAlso int

	minNumRatings int32
	maxNumRatings int32
	minRating     int32
	maxRating     int32

	maxParallelism int

	crawled *int32
	checked *int32

	runLock sync.Mutex
	start   time.Time
}

func NewCrawler(options ...CrawlerOption) *Crawler {
	var crawled int32
	var checked int32
	var inMemoryStorage = &memory.Storage{}
	inMemoryStorage.Initialize(context.Background())
	crawler := &Crawler{
		Client:         myhttp.NewClient(semaphore.NewWeighted(1), extraStatusCodesToRetry),
		Storage:        inMemoryStorage,
		maxDepth:       3,
		maxReadAlso:    5,
		maxParallelism: 1,
		minNumRatings:  -1,
		maxNumRatings:  -1,
		minRating:      -1,
		maxRating:      -1,
		crawled:        &crawled,
		checked:        &checked,
	}
	for _, option := range options {
		option(crawler)
	}
	return crawler
}

type CrawlerOption = func(*Crawler)

func WithMaxDepth(maxDepth int) CrawlerOption {
	return func(c *Crawler) {
		c.maxDepth = maxDepth
	}
}

func WithMaxReadAlso(maxReadAlso int) CrawlerOption {
	return func(c *Crawler) {
		c.maxReadAlso = maxReadAlso
	}
}

func WithMinNumRatings(minNumRatings int32) CrawlerOption {
	return func(c *Crawler) {
		c.minNumRatings = minNumRatings
	}
}

func WithMaxNumRatings(maxNumRatings int32) CrawlerOption {
	return func(c *Crawler) {
		c.maxNumRatings = maxNumRatings
	}
}

func WithMinRating(minRating int32) CrawlerOption {
	return func(c *Crawler) {
		c.minRating = minRating
	}
}

func WithMaxRating(maxRating int32) CrawlerOption {
	return func(c *Crawler) {
		c.maxRating = maxRating
	}
}

func WithMaxParallelism(maxParallelism int) CrawlerOption {
	return func(c *Crawler) {
		c.maxParallelism = maxParallelism
		c.Client.ParallelismSem = semaphore.NewWeighted(int64(maxParallelism))
	}
}

func WithRequestMaxRetries(maxRetries int) CrawlerOption {
	return func(c *Crawler) {
		c.Client.RetryMax(maxRetries)
	}
}

func WithRequestMaxRetryWait(maxWait time.Duration) CrawlerOption {
	return func(c *Crawler) {
		c.Client.RetryWaitMax(maxWait)
	}
}

func WithRequestMinRetryWait(minWait time.Duration) CrawlerOption {
	return func(c *Crawler) {
		c.Client.RetryWaitMin(minWait)
	}
}
