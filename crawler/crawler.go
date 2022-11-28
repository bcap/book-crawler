package crawler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/bcap/book-crawler/book"
	myhttp "github.com/bcap/book-crawler/http"
	"github.com/bcap/book-crawler/log"
	"github.com/bcap/book-crawler/storage"
)

var extraStatusCodesToRetry = []int{
	403, // sometimes goodreads returns 403 (Forbidden), but we should retry on it
}

type Crawler struct {
	client  *myhttp.Client
	storage storage.Storage

	maxDepth    int
	maxReadAlso int

	maxParallelism int

	currentProgress *int64
	progressTotal   int64
	crawled         *int32
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
	var inMemoryStorage = &storage.InMemoryStorage{}
	inMemoryStorage.Initialize(context.Background())
	crawler := &Crawler{
		client:          myhttp.NewClient(semaphore.NewWeighted(1), extraStatusCodesToRetry),
		maxDepth:        3,
		maxReadAlso:     5,
		currentProgress: &currentProgress,
		crawled:         &crawled,
		maxParallelism:  1,
		storage:         inMemoryStorage,
	}
	for _, option := range options {
		option(crawler)
	}
	crawler.progressTotal = calcProgressTotal(crawler.maxDepth, crawler.maxReadAlso)
	return crawler
}

func (c *Crawler) Crawl(ctx context.Context, url string) (book.Graph, error) {
	log.Infof(
		"Crawling up at most %d books in parallel, up to depth %d and following up to %d book recommendations per book. This run will potentially execute %d book checks",
		c.maxParallelism, c.maxDepth, c.maxReadAlso, c.progressTotal,
	)
	err := c.crawl(ctx, url, 0, 0)
	if err != nil {
		return book.Graph{}, err
	}

	b, err := c.storage.GetBook(ctx, url)
	if err != nil {
		return book.Graph{}, err
	}

	log.Infof(
		"Crawled %d books with %d checks. %d checks avoided",
		*c.crawled, *c.currentProgress, c.progressTotal-*c.currentProgress,
	)
	return c.storage.BookGraph(ctx, b, c.maxDepth)
}

func (c *Crawler) crawl(ctx context.Context, url string, depth int, index int) error {
	if depth > c.maxDepth {
		return nil
	}

	progress := atomic.AddInt64(c.currentProgress, 1)
	progressPct := float32(progress) / float32(c.progressTotal) * 100

	logAlreadyCrawled := func() {
		log.Debugf("[%02.1f%%, %02d/%02d] book already crawled or being crawled, skipping (%s)", progressPct, depth, index, url)
	}
	state, err := c.storage.GetBookState(ctx, url)
	if err != nil {
		return err
	}
	if state != storage.NotCrawled {
		logAlreadyCrawled()
		return nil
	}

	if set, err := c.storage.SetBookState(ctx, url, storage.NotCrawled, storage.BeingCrawled); err != nil {
		return err
	} else if !set {
		logAlreadyCrawled()
		return nil
	}

	b := book.New(url, c.maxReadAlso)

	res, err := c.client.Request(ctx, "GET", url, nil, nil)
	if err != nil {
		return err
	}

	if res.StatusCode/100 != 2 {
		err := fmt.Errorf("failed to fetch: %s returned status code %d", url, res.StatusCode)
		return err
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return err
	}

	crawled := atomic.AddInt32(c.crawled, 1)

	book.Build(b, doc)
	c.storage.SetBook(ctx, url, b)

	log.Infof(
		"[%03d, %02.1f%%, %02d/%02d] crawled book %s by %s (%s)",
		crawled, progressPct, depth, index, b.Title, b.Author, url,
	)

	alsoReadLink, hasAlsoReadLink := doc.Find("a.actionLink.seeMoreLink").Attr("href")
	if !hasAlsoReadLink {
		return errors.New("book has no related books")
	}

	alsoReadLink, err = myhttp.AbsoluteURL(url, alsoReadLink)
	if err != nil {
		return err
	}

	if depth < c.maxDepth {
		if err := c.crawlAlsoRead(ctx, url, alsoReadLink, depth); err != nil {
			return err
		}
	}

	return nil
}

func (c *Crawler) crawlAlsoRead(ctx context.Context, bookURL string, similarBooksURL string, depth int) error {
	toCrawl, err := c.extractRelatedBookURLs(ctx, similarBooksURL)
	if err != nil {
		return err
	}

	log.Debugf("extracted the following urls from %q: %v", similarBooksURL, toCrawl)

	group, ctx := errgroup.WithContext(ctx)
	for _idx, _linkURL := range toCrawl {
		idx := _idx
		linkURL := _linkURL
		group.Go(func() error {
			err := c.crawl(ctx, linkURL, depth+1, idx)
			if err != nil {
				return err
			}
			if err := c.storage.LinkBooks(ctx, bookURL, linkURL); err != nil {
				return err
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}
	return nil
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
