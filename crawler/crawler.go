package crawler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/sync/errgroup"

	"github.com/bcap/book-crawler/book"
	myhttp "github.com/bcap/book-crawler/http"
	"github.com/bcap/book-crawler/log"
	"github.com/bcap/book-crawler/storage"
)

func (c *Crawler) Crawl(ctx context.Context, url string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if !c.runLock.TryLock() {
		return errors.New("Crawl cannot be called concurrently")
	}
	defer c.runLock.Unlock()

	c.start = time.Now()

	log.Infof(
		"Crawling up at most %d books in parallel, up to depth %d and following up to %d book recommendations per book",
		c.maxParallelism, c.maxDepth, c.maxReadAlso,
	)

	go c.keepLoggingProgress(ctx)

	err := c.crawl(ctx, url, 0, 0)
	if err != nil {
		return err
	}

	c.logProgress()
	return nil
}

func (c *Crawler) keepLoggingProgress(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			c.logProgress()
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (c *Crawler) logProgress() {
	log.Infof("Crawled %d books in %d book checks", atomic.LoadInt32(c.crawled), atomic.LoadInt32(c.checked))
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

	stateChangedInCurrentRun := stateChange.When.After(c.start)

	log.Debugf(
		"url: %s, state: %v, depth: %d, index: %d, state changed: %v, state changed in current run: %v",
		url, stateChange.State, depth, index, stateChange.When, stateChangedInCurrentRun,
	)

	if stateChangedInCurrentRun {
		return nil
	}

	if stateChange.State == storage.Crawled {
		if stateChange, set, err := c.Storage.SetBookState(ctx, url, stateChange, storage.Crawled); err != nil {
			return err
		} else if !set {
			return nil
		} else {
			return c.handleCrawled(ctx, url, stateChange, depth, index, checked, nil)
		}
	}

	if stateChange.State == storage.Linked {
		if stateChange, set, err := c.Storage.SetBookState(ctx, url, stateChange, storage.Linked); err != nil {
			return err
		} else if !set {
			return nil
		} else {
			return c.handlePreviouslyLinked(ctx, url, stateChange, depth, index, checked)
		}
	}

	if stateChange, set, err := c.Storage.SetBookState(ctx, url, stateChange, storage.BeingCrawled); err != nil {
		return err
	} else if !set {
		return nil
	} else {
		return c.handleNotCrawled(ctx, url, stateChange, depth, index, checked)
	}
}

func (c *Crawler) handleNotCrawled(ctx context.Context, url string, prevState storage.StateChange, depth int, index int, checked int32) error {
	b := book.New(url)

	doc, err := c.fetch(ctx, url)
	if err != nil {
		return err
	}

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

	stateChange, set, err := c.Storage.SetBookState(ctx, url, prevState, storage.Crawled)
	if err != nil {
		return err
	} else if !set {
		return fmt.Errorf(
			"invalid state transition: book at %s could not be transitioned from state %v to %v",
			url, storage.BeingCrawled, storage.Crawled,
		)
	}

	crawled := atomic.AddInt32(c.crawled, 1)

	log.Infof(
		"[%03d, %03d, %02d/%02d] crawled book %s by %s (%s)",
		checked, crawled, depth, index, b.Title, b.Author, url,
	)

	return c.handleCrawled(ctx, url, stateChange, depth, index, checked, doc)
}

func (c *Crawler) handleCrawled(ctx context.Context, url string, prevState storage.StateChange, depth int, index int, checked int32, doc *goquery.Document) error {
	if doc == nil {
		var err error
		doc, err = c.fetch(ctx, url)
		if err != nil {
			return err
		}
	}

	alsoReadLink, hasAlsoReadLink := doc.Find("a.actionLink.seeMoreLink").Attr("href")
	if !hasAlsoReadLink {
		return errors.New("book has no related books")
	}

	alsoReadLink, err := myhttp.AbsoluteURL(url, alsoReadLink)
	if err != nil {
		return err
	}

	if depth < c.maxDepth {
		if err := c.crawlAlsoRead(ctx, url, alsoReadLink, depth); err != nil {
			return err
		}
	}

	if _, set, err := c.Storage.SetBookState(ctx, url, prevState, storage.Linked); err != nil {
		return err
	} else if !set {
		return fmt.Errorf(
			"invalid state transition: book at %s could not be transitioned from state %v to %v",
			url, storage.Crawled, storage.Linked,
		)
	}

	return nil
}

func (c *Crawler) handlePreviouslyLinked(ctx context.Context, url string, prevState storage.StateChange, depth int, index int, checked int32) error {
	b, err := c.Storage.GetBook(ctx, url, 1)
	if err != nil {
		return err
	}
	errGroup := errgroup.Group{}
	for _idx, _relatedBook := range b.AlsoRead {
		idx := _idx
		relatedURL := _relatedBook.To.URL
		errGroup.Go(func() error {
			return c.crawl(ctx, relatedURL, depth+1, idx)
		})
	}
	err = errGroup.Wait()
	return err
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
			if err := c.Storage.LinkBook(ctx, bookURL, linkURL, idx); err != nil {
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

func (c *Crawler) fetch(ctx context.Context, url string) (*goquery.Document, error) {
	res, err := c.Client.Request(ctx, "GET", url, nil, nil)
	if err != nil {
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		err := fmt.Errorf("failed to fetch: %s returned status code %d", url, res.StatusCode)
		return nil, err
	}

	return goquery.NewDocumentFromReader(res.Body)
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
