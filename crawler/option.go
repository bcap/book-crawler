package crawler

import (
	"time"

	"golang.org/x/sync/semaphore"
)

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

func WithMinRating(minRating float32) CrawlerOption {
	return func(c *Crawler) {
		c.minRating = minRating
	}
}

func WithMaxRating(maxRating float32) CrawlerOption {
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
