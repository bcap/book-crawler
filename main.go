package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	urllib "net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Stephen Hawking - A Brief History of Time
const rootBook = "https://www.goodreads.com/book/show/3869.A_Brief_History_of_Time"

func main() {
	log.Print("starting")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	crawler := NewCrawler(WithMaxDepth(3), WithMaxReadAlso(3))
	book, err := crawler.Crawl(ctx, rootBook)
	if err != nil {
		panic(err)
	}

	printGraph(book, os.Stdout)
}

type Book struct {
	Title   string
	Author  string
	Rating  int32
	Ratings int32
	Reviews int32
	URL     string

	AlsoRead []*Book
}

func (b *Book) ID() string {
	return fmt.Sprintf("%s by %s", b.Title, b.Author)
}

type Crawler struct {
	client http.Client

	maxDepth             int
	maxReadAlso          int
	crawledBookSet       map[string]*Book
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
		maxReadAlso:    10,
		crawledBookSet: make(map[string]*Book),
	}
	for _, option := range options {
		option(crawler)
	}
	return crawler
}

func (c *Crawler) Crawl(ctx context.Context, url string) (*Book, error) {
	return c.crawl(ctx, url, 0)
}

func (c *Crawler) crawl(ctx context.Context, url string, depth int) (*Book, error) {
	if depth > c.maxDepth {
		return nil, nil
	}

	c.crawledBooksSetMutex.RLock()
	if book, visited := c.crawledBookSet[url]; visited {
		c.crawledBooksSetMutex.RUnlock()
		return book, nil
	}
	c.crawledBooksSetMutex.RUnlock()

	// double checked locking
	// https://en.wikipedia.org/wiki/Double-checked_locking
	c.crawledBooksSetMutex.Lock()
	if book, visited := c.crawledBookSet[url]; visited {
		c.crawledBooksSetMutex.Unlock()
		return book, nil
	}

	book := Book{
		URL: url,
	}

	c.crawledBookSet[url] = &book
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

	if err := buildBook(&book, doc); err != nil {
		return nil, err
	}

	log.Printf("%scrawled book %s by %s (%s)", indent("  ", depth), book.Title, book.Author, url)

	alsoReadLink, hasAlsoReadLink := doc.Find("a.actionLink.seeMoreLink").Attr("href")
	if !hasAlsoReadLink {
		return &book, err
	}

	alsoReadLink, err = absoluteURL(url, alsoReadLink)
	if err != nil {
		return &book, err
	}

	alsoRead, err := c.fetchAlsoRead(ctx, alsoReadLink, depth)
	if err != nil {
		return &book, err
	}

	book.AlsoRead = alsoRead
	return &book, nil
}

func (c *Crawler) fetchAlsoRead(ctx context.Context, url string, depth int) ([]*Book, error) {
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

	results := []*Book{}

	resultsLock := sync.Mutex{}
	group, ctx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(1)

	crawling := 0
	for _, linkURL := range urls {
		if crawling == c.maxReadAlso {
			break
		}

		linkURL, err := absoluteURL(url, linkURL)
		if err != nil {
			return nil, err
		}

		if !strings.Contains(linkURL, "/book/show/") {
			continue
		}

		c.crawledBooksSetMutex.RLock()
		if fetchedBook := c.crawledBookSet[linkURL]; fetchedBook != nil {
			results = append(results, fetchedBook)
			c.crawledBooksSetMutex.RUnlock()
			continue
		}
		c.crawledBooksSetMutex.RUnlock()

		crawling++
		group.Go(func() error {
			sem.Acquire(ctx, 1)
			defer sem.Release(1)

			book, err := c.crawl(ctx, linkURL, depth+1)
			if err != nil {
				return err
			}
			if book != nil {
				resultsLock.Lock()
				results = append(results, book)
				resultsLock.Unlock()
			}
			return nil
		})
	}
	return results, group.Wait()
}

func buildBook(book *Book, doc *goquery.Document) error {
	selection := doc.Find("h1#bookTitle")
	if selection.Length() == 0 {
		return errors.New("book has no title")
	}
	book.Title = cleanHTMLText(selection.Eq(0).Text())

	selection = doc.Find("a.authorName span")
	if selection.Length() == 0 {
		return errors.New("book has no author")
	}
	book.Author = cleanHTMLText(selection.Eq(0).Text())

	selection = doc.Find("span[itemprop=ratingValue]")
	if selection.Length() == 0 {
		return errors.New("book has no rating")
	}
	ratingStr := cleanHTMLText(selection.Eq(0).Text())
	ratingFloat64, err := strconv.ParseFloat(ratingStr, 64)
	if err != nil {
		return errors.New("book rating is not a float")
	}
	book.Rating = int32(ratingFloat64 * 100)

	ratingsStr, has := doc.Find("a meta[itemprop=ratingCount]").Attr("content")
	if !has {
		return errors.New("book has no number of ratings available")
	}
	ratings, err := strconv.Atoi(ratingsStr)
	if err != nil {
		return errors.New("book number of reviews is not an integer")
	}
	book.Ratings = int32(ratings)

	reviewsStr, has := doc.Find("a meta[itemprop=reviewCount]").Attr("content")
	if !has {
		return errors.New("book has no number of reviews available")
	}
	reviews, err := strconv.Atoi(reviewsStr)
	if err != nil {
		return errors.New("book number of reviews is not an integer")
	}
	book.Reviews = int32(reviews)

	return nil
}

func request(ctx context.Context, client http.Client, method string, url string, body io.Reader) (*http.Response, error) {
	debugVar := os.Getenv("DEBUG")
	debug := debugVar != "" && debugVar != "0"

	var res *http.Response
	for {
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			log.Printf("%s %s failed with error: %v", method, url, err)
			return nil, err
		}
		res, err = client.Do(req)
		if err != nil {
			log.Printf("%s %s failed with error: %v", method, url, err)
			return nil, err
		}
		visitedLocations := map[string]struct{}{
			url: {},
		}
		if debug {
			log.Printf("%s %s returned status code %d", method, url, res.StatusCode)
		}
		if res.StatusCode == 301 || res.StatusCode == 302 {
			location := res.Header.Get("location")
			if location == "" {
				location = res.Header.Get("Location")
			}
			if _, visited := visitedLocations[location]; visited {
				err := fmt.Errorf("redirect loop detected while following %s", url)
				return nil, err
			}
			if location == "" {
				err := fmt.Errorf("cannot follow redirect: %s returned status code %d but no location header set", url, res.StatusCode)
				return nil, err
			}
			visitedLocations[url] = struct{}{}
			method = "GET"
			url = location
			continue
		}
		break
	}

	if debug {
		bodyBytes, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return nil, err
		}
		log.Print(string(bodyBytes))
		res.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	return res, nil
}

func printGraph(book *Book, writer io.Writer) {
	visited := map[string]struct{}{}
	var printBook func(book *Book) = nil
	printBook = func(book *Book) {
		visited[book.ID()] = struct{}{}
		for _, relatedBook := range book.AlsoRead {
			fmt.Fprintf(writer, "%q -> %q\n", book.ID(), relatedBook.ID())
		}
		for _, relatedBook := range book.AlsoRead {
			if _, v := visited[relatedBook.ID()]; !v {
				printBook(relatedBook)
			}
		}
	}
	fmt.Fprint(writer, "digraph G {\n")
	printBook(book)
	fmt.Fprint(writer, "}\n")
}

func cleanHTMLText(text string) string {
	nbsp := string([]byte{194, 160})
	text = strings.ReplaceAll(text, nbsp, " ")
	text = strings.TrimSpace(text)
	return text
}

func absoluteURL(baseURL string, url string) (string, error) {
	parsedBaseURL, err := urllib.Parse(baseURL)
	if err != nil {
		return "", err
	}

	parsedURL, err := urllib.Parse(url)
	if err != nil {
		return "", err
	}

	// in case it is a relative URL, make it absolute
	if parsedURL.Host == "" {
		parsedURL.Host = parsedBaseURL.Host
		parsedURL.Scheme = parsedBaseURL.Scheme
	}

	return parsedURL.String(), nil
}

func indent(symbol string, times int) string {
	builder := strings.Builder{}
	for i := 0; i < times; i++ {
		builder.WriteString(symbol)
	}
	return builder.String()
}
