package book

import (
	"regexp"
	"strconv"

	"github.com/PuerkitoBio/goquery"
	"github.com/bcap/book-crawler/html"
)

var ratingsRegex = regexp.MustCompile(`title=\\"(\d+) ratings\\"`)
var pagesRegex = regexp.MustCompile(`(\d+) pages`)

func Build(book *Book, doc *goquery.Document) {
	book.Title = extractTitle(doc)
	book.Author = extractAuthor(doc)
	book.AuthorURL = extractAuthorURL(doc)
	book.Rating = extractRating(doc)
	book.RatingsTotal = extractNumRatingsTotal(doc)
	ratingsByStar := extractNumRatingsByStars(doc)
	book.Ratings1 = ratingsByStar[1]
	book.Ratings2 = ratingsByStar[2]
	book.Ratings3 = ratingsByStar[3]
	book.Ratings4 = ratingsByStar[4]
	book.Ratings5 = ratingsByStar[5]
	book.Reviews = extractNumReviews(doc)
	book.Pages = extractNumPages(doc)
	book.Genres = extractGenres(doc)
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

func extractAuthorURL(doc *goquery.Document) string {
	selection := doc.Find("a.authorName")
	if selection.Length() == 0 {
		return ""
	}
	return html.CleanText(selection.AttrOr("href", ""))
}

func extractRating(doc *goquery.Document) int32 {
	selection := doc.Find("span[itemprop=ratingValue]")
	if selection.Length() == 0 {
		return -1
	}
	ratingStr := html.CleanText(selection.Eq(0).Text())
	ratingFloat, err := strconv.ParseFloat(ratingStr, 32)
	if err != nil {
		return -1
	}
	return int32(ratingFloat * 100)
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

func extractNumRatingsByStars(doc *goquery.Document) map[int]int32 {
	// The following is super ugly
	// The ratings by level are given by an ugly javascript inside a
	// <script> + CDATA tags
	// Here we use regexes to find such data
	key := "a#rating_details + script"
	ratingsScript := doc.Find(key).Text()
	matches := ratingsRegex.FindAllStringSubmatch(ratingsScript, -1)
	results := map[int]int32{}
	for idx, match := range matches {
		rating, err := strconv.Atoi(match[1])
		if err != nil {
			rating = -1
		}
		results[5-idx] = int32(rating)
	}
	return results
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

func extractNumPages(doc *goquery.Document) int32 {
	pagesStr := doc.Find("div#details div.row span[itemprop=numberOfPages]").Text()
	matches := pagesRegex.FindStringSubmatch(pagesStr)
	if len(matches) < 2 {
		return -1
	}
	pages, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1
	}
	return int32(pages)
}

func extractGenres(doc *goquery.Document) []string {
	sel := doc.Find("a.bookPageGenreLink")
	genres := make([]string, sel.Length())
	sel.Each(func(i int, s *goquery.Selection) {
		genres[i] = s.Text()
	})
	return genres
}
