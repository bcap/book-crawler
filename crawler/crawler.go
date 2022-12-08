package crawler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/PuerkitoBio/goquery"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/sync/errgroup"

	"github.com/bcap/book-crawler/book"
	myhttp "github.com/bcap/book-crawler/http"
	"github.com/bcap/book-crawler/log"
	"github.com/bcap/book-crawler/storage"
)

var extraStatusCodesToRetry = []int{
	403, // sometimes goodreads returns 403 (Forbidden), but we should retry on it
}

func (c *Crawler) Crawl(ctx context.Context, url string) error {
	log.Infof(
		"Crawling up at most %d books in parallel, up to depth %d and following up to %d book recommendations per book",
		c.maxParallelism, c.maxDepth, c.maxReadAlso,
	)

	err := c.crawl(ctx, url, 0, 0)
	if err != nil {
		return err
	}

	log.Infof("Crawled %d books with %d checks", *c.crawled, *c.checked)
	return nil
}

func (c *Crawler) crawl(ctx context.Context, url string, depth int, index int) error {
	if depth > c.maxDepth {
		return nil
	}

	checked := atomic.AddInt32(c.checked, 1)

	stateChange, err := c.Storage.GetBookState(ctx, url)
	if err != nil {
		return err
	}

	if stateChange.State == storage.BeingCrawled {
		return nil
	}

	if stateChange.State == storage.Linked {
		b, err := c.Storage.GetBook(ctx, url, 1)
		if err != nil {
			return err
		}
		errGroup := errgroup.Group{}
		for _idx, _relatedBook := range b.AlsoRead {
			idx := _idx
			relatedURL := _relatedBook.URL
			errGroup.Go(func() error {
				return c.crawl(ctx, relatedURL, depth+1, idx)
			})
		}
		return errGroup.Wait()
	}

	if set, err := c.Storage.SetBookState(ctx, url, storage.NotCrawled, storage.BeingCrawled); err != nil {
		return err
	} else if !set {
		log.Debugf(
			"[%03d %02d/%02d] book already being crawled by some other goroutine, skipping (%s)",
			checked, depth, index, url,
		)
		return nil
	}

	b := book.New(url)

	res, err := c.Client.Request(ctx, "GET", url, nil, nil)
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

	if (c.minNumRatings >= 0 && b.RatingsTotal < c.minNumRatings) ||
		(c.maxNumRatings >= 0 && b.RatingsTotal > c.maxNumRatings) ||
		(c.minRating >= 0 && b.Rating < c.minRating) ||
		(c.maxRating >= 0 && b.Rating > c.maxRating) {
		return nil
	}

	if err := c.Storage.SetBook(ctx, url, b); err != nil {
		return err
	}

	if set, err := c.Storage.SetBookState(ctx, url, storage.BeingCrawled, storage.Crawled); err != nil {
		return err
	} else if !set {
		return fmt.Errorf(
			"invalid state transition: book at %s could not be transitioned from state %v to %v",
			url, storage.BeingCrawled, storage.Crawled,
		)
	}

	log.Infof(
		"[%03d, %03d, %02d/%02d] crawled book %s by %s (%s)",
		checked, crawled, depth, index, b.Title, b.Author, url,
	)
	log.Debugf("crawled book: %s", spew.Sdump(b))

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

	if set, err := c.Storage.SetBookState(ctx, url, storage.Crawled, storage.Linked); err != nil {
		return err
	} else if !set {
		return fmt.Errorf(
			"invalid state transition: book at %s could not be transitioned from state %v to %v",
			url, storage.Crawled, storage.Linked,
		)
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
			if err := c.Storage.LinkBooks(ctx, bookURL, linkURL); err != nil {
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
	resp, err := c.Client.Request(ctx, "GET", url, nil, nil)
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
