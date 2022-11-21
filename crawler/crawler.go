package crawler

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	myhttp "github.com/bcap/book-crawler/http"
	"github.com/bcap/book-crawler/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var extraStatusCodesToRetry = []int{
	403, // sometimes goodreads returns 403 (Forbidden), but we should retry on it
}

type Crawler struct {
	client *myhttp.Client

	maxDepth    int
	maxReadAlso int

	currentProgress *int64
	progressTotal   int64

	crawled              *int32
	crawledBookSet       map[string]*Book
	crawledBooksSetMutex sync.RWMutex

	maxParallelism int
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

func WithMaxParallelism(maxParallelism int) CrawlerOption {
	return func(c *Crawler) {
		c.maxParallelism = maxParallelism
		c.client.ParallelismSem = semaphore.NewWeighted(int64(maxParallelism))
	}
}

func WithRequestMaxRetries(maxRetries int) CrawlerOption {
	return func(c *Crawler) {
		c.client.RetryMax(maxRetries)
	}
}

func WithRequestMaxRetryWait(maxWait time.Duration) CrawlerOption {
	return func(c *Crawler) {
		c.client.RetryWaitMax(maxWait)
	}
}

func WithRequestMinRetryWait(minWait time.Duration) CrawlerOption {
	return func(c *Crawler) {
		c.client.RetryWaitMin(minWait)
	}
}

func NewCrawler(options ...CrawlerOption) *Crawler {
	var currentProgress int64
	var crawled int32
	crawler := &Crawler{
		client:          myhttp.NewClient(semaphore.NewWeighted(1), extraStatusCodesToRetry),
		maxDepth:        3,
		maxReadAlso:     5,
		currentProgress: &currentProgress,
		crawled:         &crawled,
		maxParallelism:  1,
		crawledBookSet:  make(map[string]*Book),
	}
	for _, option := range options {
		option(crawler)
	}
	crawler.progressTotal = calcProgressTotal(crawler.maxDepth, crawler.maxReadAlso)
	return crawler
}

func (c *Crawler) Crawl(ctx context.Context, url string) (BookGraph, error) {
	log.Infof(
		"Crawling up at most %d books in parallel, up to depth %d and following up to %d book recommendations per book. This run will potentially execute %d book checks",
		c.maxParallelism, c.maxDepth, c.maxReadAlso, c.progressTotal,
	)
	book, err := c.crawl(ctx, url, 0, 0)
	if err != nil {
		return BookGraph{}, err
	}

	log.Infof(
		"Crawled %d books with %d checks. %d checks avoided",
		*c.crawled, *c.currentProgress, c.progressTotal-*c.currentProgress,
	)
	return BookGraph{
		Root:    book,
		All:     collectBooks(book),
		ByDepth: collectBooksByDepth(book),
	}, nil
}

func (c *Crawler) crawl(ctx context.Context, url string, depth int, index int) (*Book, error) {
	if depth > c.maxDepth {
		return nil, nil
	}

	progress := atomic.AddInt64(c.currentProgress, 1)
	progressPct := float32(progress) / float32(c.progressTotal) * 100

	logAlreadyCrawled := func() {
		log.Debugf("[%02.1f%%, %02d/%02d] book already crawled or being crawled, skipping (%s)", progressPct, depth, index, url)
	}

	c.crawledBooksSetMutex.RLock()
	if book, visited := c.crawledBookSet[url]; visited {
		c.crawledBooksSetMutex.RUnlock()
		logAlreadyCrawled()
		return book, nil
	}
	c.crawledBooksSetMutex.RUnlock()

	// double checked locking
	// https://en.wikipedia.org/wiki/Double-checked_locking
	c.crawledBooksSetMutex.Lock()
	if book, visited := c.crawledBookSet[url]; visited {
		c.crawledBooksSetMutex.Unlock()
		logAlreadyCrawled()
		return book, nil
	}

	book := Book{
		URL: url,
	}

	c.crawledBookSet[url] = &book
	c.crawledBooksSetMutex.Unlock()

	res, err := c.client.Request(ctx, "GET", url, nil, nil)
	if err != nil {
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		err := fmt.Errorf("failed to fetch: %s returned status code %d", url, res.StatusCode)
		return nil, err
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, err
	}

	crawled := atomic.AddInt32(c.crawled, 1)
	buildBook(&book, doc)

	log.Infof(
		"[%03d, %02.1f%%, %02d/%02d] crawled book %s by %s (%s)",
		crawled, progressPct, depth, index, book.Title, book.Author, url,
	)

	alsoReadLink, hasAlsoReadLink := doc.Find("a.actionLink.seeMoreLink").Attr("href")
	if !hasAlsoReadLink {
		return &book, err
	}

	alsoReadLink, err = myhttp.AbsoluteURL(url, alsoReadLink)
	if err != nil {
		return &book, err
	}

	if depth < c.maxDepth {
		alsoRead, err := c.fetchAlsoRead(ctx, alsoReadLink, depth)
		if err != nil {
			return &book, err
		}
		book.AlsoRead = alsoRead
	}

	return &book, nil
}

func (c *Crawler) fetchAlsoRead(ctx context.Context, url string, depth int) ([]*Book, error) {
	toCrawl, err := c.extractRelatedBookURLs(ctx, url)
	if err != nil {
		return nil, err
	}

	group, ctx := errgroup.WithContext(ctx)
	results := make([]*Book, len(toCrawl))
	for _idx, _linkURL := range toCrawl {
		idx := _idx
		linkURL := _linkURL
		group.Go(func() error {
			book, err := c.crawl(ctx, linkURL, depth+1, idx)
			if err != nil {
				return err
			}
			results[idx] = book
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (c *Crawler) extractRelatedBookURLs(ctx context.Context, url string) ([]string, error) {
	resp, err := c.client.Request(ctx, "GET", url, nil, nil)
	if err != nil {
		return nil, err
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}

	urls := []string{}
	doc.Find("div.responsiveMainContentContainer div.membersAlsoLikedText").
		NextAll().
		Find("a[itemprop=url]").
		Each(func(_ int, node *goquery.Selection) {
			if len(urls) == c.maxReadAlso {
				return
			}
			linkURL, hasUrl := node.Attr("href")
			if !hasUrl {
				return
			}
			absoluteLinkURL, err := myhttp.AbsoluteURL(url, linkURL)
			if err != nil {
				log.Warnf("found bad url, skipping it: %s", linkURL)
				return
			}
			if !strings.Contains(absoluteLinkURL, "/book/show/") {
				return
			}
			urls = append(urls, absoluteLinkURL)
		})

	return urls, nil
}

func calcProgressTotal(depth int, readAlso int) int64 {
	// this is basically the size of a complete tree
	// readAlso^0 + readAlso^1 + readAlso^2 + ... + readAlso^depth
	var result int64 = 1
	for i := 1; i <= depth; i++ {
		result += int64(math.Pow(float64(readAlso), float64(i)))
	}
	return result
}
