package http

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/bcap/book-crawler/log"
	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/sync/semaphore"
)

type Client struct {
	client                  retryablehttp.Client
	ParallelismSem          *semaphore.Weighted
	ExtraStatusCodesToRetry []int
}

func NewClient(
	parallelismSem *semaphore.Weighted,
	extraStatusCodesToRetry []int,
) *Client {
	c := Client{
		client:                  *retryablehttp.NewClient(),
		ParallelismSem:          parallelismSem,
		ExtraStatusCodesToRetry: extraStatusCodesToRetry,
	}
	c.client.CheckRetry = c.checkRetry
	c.client.Logger = debugLogger{}
	return &c
}

func (c *Client) RetryMax(retries int) {
	c.client.RetryMax = retries
}

func (c *Client) RetryWaitMin(duration time.Duration) {
	c.client.RetryWaitMin = duration
}

func (c *Client) RetryWaitMax(duration time.Duration) {
	c.client.RetryWaitMax = duration
}

func (c *Client) Request(ctx context.Context, method string, url string, header http.Header, body io.Reader) (*http.Response, error) {
	req, err := retryablehttp.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = header
	}
	if c.ParallelismSem != nil {
		if err := c.ParallelismSem.Acquire(ctx, 1); err != nil {
			return nil, err
		}
		defer c.ParallelismSem.Release(1)
	}
	return c.client.Do(req)
}

func (c *Client) checkRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	// base policy retry + logging
	should, policyErr := retryablehttp.ErrorPropagatedRetryPolicy(ctx, resp, err)
	if policyErr != nil {
		return should, policyErr
	}
	if should {
		if err != nil {
			log.Warnf("retrying request to %s: %s", resp.Request.URL, err)
		} else {
			log.Warnf("retrying request to %s: got status code %d", resp.Request.URL, resp.StatusCode)
		}
		return true, nil
	}

	// custom retry logic
	if resp == nil || err != nil {
		return false, err
	}
	for _, code := range c.ExtraStatusCodesToRetry {
		if code == resp.StatusCode {
			log.Warnf("retrying request to %s: got status code %d", resp.Request.URL, code)
			return true, nil
		}
	}
	return false, nil
}

type debugLogger struct{}

func (debugLogger) Printf(msg string, v ...any) {
	log.Debugf(msg, v...)
}
