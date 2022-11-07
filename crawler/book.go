package crawler

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type BookGraph struct {
	Root    *Book
	All     []*Book
	ByDepth [][]*Book
}

type Book struct {
	Title   string
	Author  string
	Rating  int32
	Ratings int32
	Reviews int32
	URL     string

	AlsoRead  []*Book
	alsoReadP []**Book
}

func (b *Book) String() string {
	return fmt.Sprintf("%s by %s", b.Title, b.Author)
}

func buildBook(book *Book, doc *goquery.Document) {
	book.Title = extractTitle(doc)
	book.Author = extractAuthor(doc)
	book.Rating = extractRating(doc)
	book.Ratings = extractNumRatings(doc)
	book.Reviews = extractNumReviews(doc)
}

func extractTitle(doc *goquery.Document) string {
	selection := doc.Find("h1#bookTitle")
	if selection.Length() == 0 {
		return ""
	}
	return cleanHTMLText(selection.Eq(0).Text())
}

func extractAuthor(doc *goquery.Document) string {
	selection := doc.Find("a.authorName span")
	if selection.Length() == 0 {
		return ""
	}
	return cleanHTMLText(selection.Eq(0).Text())
}

func extractRating(doc *goquery.Document) int32 {
	selection := doc.Find("span[itemprop=ratingValue]")
	if selection.Length() == 0 {
		return -1
	}
	ratingStr := cleanHTMLText(selection.Eq(0).Text())
	ratingFloat64, err := strconv.ParseFloat(ratingStr, 64)
	if err != nil {
		return -1
	}
	return int32(ratingFloat64 * 100)
}

func extractNumRatings(doc *goquery.Document) int32 {
	ratingsStr, has := doc.Find("a meta[itemprop=ratingCount]").Attr("content")
	if !has {
		return -1
	}
	ratings, err := strconv.Atoi(ratingsStr)
	if err != nil {
		return -1
	}
	return int32(ratings)
}

func extractNumReviews(doc *goquery.Document) int32 {
	reviewsStr, has := doc.Find("a meta[itemprop=reviewCount]").Attr("content")
	if !has {
		return -1
	}
	reviews, err := strconv.Atoi(reviewsStr)
	if err != nil {
		return -1
	}
	return int32(reviews)
}

func dereferenceBook(bookP **Book) *Book {
	visited := make(map[*Book]struct{})

	var recurse func(bookP **Book)
	recurse = func(bookP **Book) {
		if bookP == nil {
			return
		}

		book := *bookP

		if book == nil {
			return
		}

		visited[book] = struct{}{}

		if book.alsoReadP == nil {
			book.AlsoRead = []*Book{}
			return
		}

		book.AlsoRead = make([]*Book, 0, len(book.alsoReadP))
		for _, alsoReadBookP := range book.alsoReadP {
			alsoReadBook := *alsoReadBookP
			book.AlsoRead = append(book.AlsoRead, alsoReadBook)
			if _, alreadyVisited := visited[alsoReadBook]; !alreadyVisited {
				recurse(alsoReadBookP)
			}
		}
		book.alsoReadP = nil
	}

	recurse(bookP)
	return *bookP
}

func collectBooks(root *Book) []*Book {
	bookMap := make(map[*Book]struct{})
	var recurse func(*Book)
	recurse = func(book *Book) {
		bookMap[book] = struct{}{}
		for _, also := range book.AlsoRead {
			if _, has := bookMap[also]; !has {
				recurse(also)
			}
		}
	}
	recurse(root)

	books := make([]*Book, len(bookMap))
	idx := 0
	for book := range bookMap {
		books[idx] = book
		idx++
	}
	sort.Slice(
		books,
		func(i int, j int) bool {
			return strings.Compare(books[i].String(), books[j].String()) < 0
		},
	)
	return books
}

func collectBooksByDepth(root *Book) [][]*Book {
	depthMap := map[*Book]int{}
	maxDepth := 0
	maxDepthP := &maxDepth
	var recurse func(book *Book, depth int)
	recurse = func(book *Book, depth int) {
		if *maxDepthP < depth {
			*maxDepthP = depth
		}
		currentDepth, has := depthMap[book]
		if !has || depth < currentDepth {
			depthMap[book] = depth
		}
		if has {
			return
		}
		for _, relatedBook := range book.AlsoRead {
			recurse(relatedBook, depth+1)
		}
	}
	recurse(root, 0)

	booksByDepth := make([][]*Book, maxDepth+1)
	for book, depth := range depthMap {
		booksByDepth[depth] = append(booksByDepth[depth], book)
	}
	return booksByDepth
}
