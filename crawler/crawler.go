package crawler

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/bcap/book-crawler/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Crawler struct {
	client http.Client

	maxDepth             int
	maxReadAlso          int
	crawledBookSet       map[string]*Book
	crawledBooksSetMutex sync.RWMutex
	maxParallelism       int
	parallelismSem       *semaphore.Weighted
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
		c.parallelismSem = semaphore.NewWeighted(int64(maxParallelism))
	}
}

func NewCrawler(options ...CrawlerOption) *Crawler {
	crawler := &Crawler{
		maxDepth:       3,
		maxReadAlso:    5,
		maxParallelism: 1,
		parallelismSem: semaphore.NewWeighted(1),
		crawledBookSet: make(map[string]*Book),
	}
	for _, option := range options {
		option(crawler)
	}
	return crawler
}

func (c *Crawler) Crawl(ctx context.Context, url string) (BookGraph, error) {
	log.Infof(
		"Crawling up at most %d books in parallel, up to depth %d and following up to %d book recommendations per book",
		c.maxParallelism, c.maxDepth, c.maxReadAlso,
	)
	book, err := c.crawl(ctx, url, 0, 0)
	if err != nil {
		return BookGraph{}, err
	}
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

	logAlreadyCrawled := func() {
		log.Debugf("(%02d/%02d) book already crawled or being crawled, skipping (%s)", depth, index, url)
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

	res, err := request(ctx, c.client, "GET", url, nil, c.parallelismSem)
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

	buildBook(&book, doc)

	log.Infof("(%02d/%02d) crawled book %s by %s (%s)", depth, index, book.Title, book.Author, url)

	alsoReadLink, hasAlsoReadLink := doc.Find("a.actionLink.seeMoreLink").Attr("href")
	if !hasAlsoReadLink {
		return &book, err
	}

	alsoReadLink, err = absoluteURL(url, alsoReadLink)
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
	resp, err := request(ctx, c.client, "GET", url, nil, c.parallelismSem)
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
			absoluteLinkURL, err := absoluteURL(url, linkURL)
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
