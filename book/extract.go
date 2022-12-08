package book

import (
	"fmt"
	"strconv"

	"github.com/PuerkitoBio/goquery"
	"github.com/bcap/book-crawler/html"
)

func Build(book *Book, doc *goquery.Document) {
	book.Title = extractTitle(doc)
	book.Author = extractAuthor(doc)
	book.Rating = extractRating(doc)
	book.RatingsTotal = extractNumRatingsTotal(doc)
	book.Ratings1 = extractNumRatings(doc, 1)
	book.Ratings2 = extractNumRatings(doc, 2)
	book.Ratings3 = extractNumRatings(doc, 3)
	book.Ratings4 = extractNumRatings(doc, 4)
	book.Ratings5 = extractNumRatings(doc, 5)
	book.Reviews = extractNumReviews(doc)
}

func extractTitle(doc *goquery.Document) string {
	selection := doc.Find("h1#bookTitle")
	if selection.Length() == 0 {
		return ""
	}
	return html.CleanText(selection.Eq(0).Text())
}

func extractAuthor(doc *goquery.Document) string {
	selection := doc.Find("a.authorName span")
	if selection.Length() == 0 {
		return ""
	}
	return html.CleanText(selection.Eq(0).Text())
}

func extractRating(doc *goquery.Document) float32 {
	selection := doc.Find("span[itemprop=ratingValue]")
	if selection.Length() == 0 {
		return -1
	}
	ratingStr := html.CleanText(selection.Eq(0).Text())
	ratingFloat, err := strconv.ParseFloat(ratingStr, 32)
	if err != nil {
		return -1
	}
	return float32(ratingFloat)
}

func extractNumRatingsTotal(doc *goquery.Document) int32 {
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

func extractNumRatings(doc *goquery.Document, which int) int32 {
	// TODO fix ratings per score
	// This currently does not work as these html elements are generated by JS when the browser
	// is rendering the page
	key := fmt.Sprintf("table#rating_distribution > tbody > tr:nth-child(%d) > th", 6-which)
	ratingsStr := doc.Find(key).Text()
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
