package crawler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	urllib "net/url"

	"github.com/bcap/book-crawler/log"
	"golang.org/x/sync/semaphore"
)

func request(ctx context.Context, client http.Client, method string, url string, body io.Reader, semaphore *semaphore.Weighted) (*http.Response, error) {
	var res *http.Response
	for {
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			log.Warnf("%s %s failed with error: %v", method, url, err)
			return nil, err
		}

		if semaphore != nil {
			semaphore.Acquire(ctx, 1)
		}
		res, err = client.Do(req)
		if semaphore != nil {
			semaphore.Release(1)
		}

		if err != nil {
			log.Warnf("%s %s failed with error: %v", method, url, err)
			return nil, err
		}
		visitedLocations := map[string]struct{}{
			url: {},
		}
		if log.Level == log.DebugLevel {
			log.Warnf("%s %s returned status code %d", method, url, res.StatusCode)
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

	if log.Level == log.DebugLevel {
		bodyBytes, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return nil, err
		}
		log.Debug(string(bodyBytes))
		res.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	return res, nil
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
