package main

import (
	"fmt"
	"os"

	"github.com/bcap/book-crawler/book"
	"github.com/bcap/book-crawler/log"

	"github.com/PuerkitoBio/goquery"
	"github.com/davecgh/go-spew/spew"
)

func main() {
	log.Level = log.DebugLevel

	b := book.Book{}

	// f, err := os.OpenFile("/Users/bcap/code/github.com/bcap/book-crawler/gone-girl.html", os.O_RDONLY, 0)
	f, err := os.OpenFile("/Users/bcap/code/github.com/bcap/book-crawler/diddly-squat.html", os.O_RDONLY, 0)
	if err != nil {
		panic(err.Error())
	}

	doc, err := goquery.NewDocumentFromReader(f)
	if err != nil {
		panic(err.Error())
	}

	book.Build(&b, doc)
	fmt.Println(spew.Sdump(b))

	genres := []string{}
	doc.Find("a.bookPageGenreLink").Each(func(i int, s *goquery.Selection) {
		genres = append(genres, s.Text())
	})
	fmt.Println(genres)

	fmt.Println(doc.Find("div#details div.row span[itemprop=numberOfPages]").Text())
	fmt.Println()
}
