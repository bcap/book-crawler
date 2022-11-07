package crawler

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/bcap/book-crawler/log"
)

type Crawler struct {
	client http.Client

	maxDepth             int
	maxReadAlso          int
	crawledBookSet       map[string]**Book
	crawledBooksSetMutex sync.RWMutex
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

func NewCrawler(options ...CrawlerOption) *Crawler {
	crawler := &Crawler{
		maxDepth:       3,
		maxReadAlso:    5,
		crawledBookSet: make(map[string]**Book),
	}
	for _, option := range options {
		option(crawler)
	}
	return crawler
}

func (c *Crawler) Crawl(ctx context.Context, url string) (BookGraph, error) {
	log.Infof("Crawling up to depth %d and following up to %d book recommendations per book", c.maxDepth, c.maxReadAlso)
	bookP, err := c.crawl(ctx, url, 0, 0)
	if err != nil {
		return BookGraph{}, err
	}
	book := dereferenceBook(bookP)
	return BookGraph{
		Root:    book,
		All:     collectBooks(book),
		ByDepth: collectBooksByDepth(book),
	}, nil
}

func (c *Crawler) crawl(ctx context.Context, url string, depth int, index int) (**Book, error) {
	if depth > c.maxDepth {
		return nil, nil
	}

	logAlreadyCrawled := func() { log.Infof("(%02d/%02d) already crawled, skipping (%s)", depth, index, url) }

	c.crawledBooksSetMutex.RLock()
	if bookP, visited := c.crawledBookSet[url]; visited {
		c.crawledBooksSetMutex.RUnlock()
		logAlreadyCrawled()
		return bookP, nil
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
	bookP := &book

	c.crawledBookSet[url] = &bookP
	c.crawledBooksSetMutex.Unlock()

	res, err := request(ctx, c.client, "GET", url, nil)
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
		return &bookP, err
	}

	alsoReadLink, err = absoluteURL(url, alsoReadLink)
	if err != nil {
		return &bookP, err
	}

	if depth < c.maxDepth {
		alsoRead, err := c.fetchAlsoRead(ctx, alsoReadLink, depth)
		if err != nil {
			return &bookP, err
		}
		book.alsoReadP = alsoRead
	}

	return &bookP, nil
}

func (c *Crawler) fetchAlsoRead(ctx context.Context, url string, depth int) ([]**Book, error) {
	resp, err := request(ctx, c.client, "GET", url, nil)
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
			if url, hasUrl := node.Attr("href"); hasUrl {
				urls = append(urls, url)
			}
		})

	results := []**Book{}
	for _, linkURL := range urls {
		if len(results) == c.maxReadAlso {
			break
		}

		linkURL, err := absoluteURL(url, linkURL)
		if err != nil {
			return nil, err
		}

		if !strings.Contains(linkURL, "/book/show/") {
			continue
		}

		// c.crawledBooksSetMutex.RLock()
		// if fetchedBook := c.crawledBookSet[linkURL]; fetchedBook != nil {
		// 	results = append(results, fetchedBook)
		// 	c.crawledBooksSetMutex.RUnlock()
		// 	continue
		// }
		// c.crawledBooksSetMutex.RUnlock()

		book, err := c.crawl(ctx, linkURL, depth+1, len(results))
		if err != nil {
			return nil, err
		}
		if book != nil {
			results = append(results, book)
		}
	}

	return results, err
}

func indent(symbol string, times int) string {
	builder := strings.Builder{}
	for i := 0; i < times; i++ {
		builder.WriteString(symbol)
	}
	return builder.String()
}
