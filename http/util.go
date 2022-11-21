package http

import (
	urllib "net/url"
)

func AbsoluteURL(baseURL string, url string) (string, error) {
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
