package dot

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/bcap/book-crawler/crawler"
)

type recurseFn = func(visited map[*crawler.Book]struct{}, book *crawler.Book, depth int)

func PrintBookGraph(graph crawler.BookGraph, writer io.Writer) {
	// analysis := analyzeGraph(graph)

	genNodes := func() {
		for depth, books := range graph.ByDepth {
			for _, book := range books {
				label := fmt.Sprintf(
					"%s\\l%s\\l%0.1f (%d ratings)\\l%d reviews\\ldepth:%d\\l",
					book.Title,
					book.Author,
					float32(book.Rating)/100.0,
					book.Ratings,
					book.Reviews,
					depth,
				)
				fmt.Fprintf(
					writer,
					"%q [nojustify=false label=\"%s\" URL=\"%s\"]\n",
					book,
					label,
					book.URL,
				)
			}
		}
	}

	genRanks := func() {
		for depth, books := range graph.ByDepth {
			rank := "same"
			if depth == 0 {
				rank = "source"
			}
			nodes := make([]string, len(books))
			for idx, book := range books {
				nodes[idx] = fmt.Sprintf("\"%s\"", book.String())
			}
			fmt.Fprintf(writer, "{rank=%s; %s}\n", rank, strings.Join(nodes, "; "))
		}
	}

	var genEdges recurseFn
	genEdges = func(visited map[*crawler.Book]struct{}, book *crawler.Book, depth int) {
		visited[book] = struct{}{}
		for idx, relatedBook := range book.AlsoRead {
			label := fmt.Sprintf("idx:%d", idx)
			fmt.Fprintf(writer, "%q -> %q [label=%q]\n", book, relatedBook, label)
		}
		for _, relatedBook := range book.AlsoRead {
			if _, v := visited[relatedBook]; !v {
				genEdges(visited, relatedBook, depth+1)
			}
		}
	}

	fmt.Fprint(writer, "digraph G {\n")
	fmt.Fprint(writer, "\n// styling\n")
	fmt.Fprint(writer, "rankdir=LR\n")
	fmt.Fprint(writer, "splines=ortho\n")
	fmt.Fprint(writer, "node [shape=box]\n")

	fmt.Fprint(writer, "\n// node declarations\n")
	genNodes()

	fmt.Fprint(writer, "\n// rank adjustments\n")
	genRanks()

	fmt.Fprint(writer, "\n// edges\n")
	genEdges(map[*crawler.Book]struct{}{}, graph.Root, 0)

	fmt.Fprint(writer, "\n}\n")
}

type analysis struct {
	minReviews int32
	maxReviews int32
	minRatings int32
	maxRatings int32

	booksByDepth [][]*crawler.Book
}

func analyzeGraph(graph crawler.BookGraph) analysis {
	result := analysis{
		minReviews:   math.MaxInt32,
		minRatings:   math.MaxInt32,
		maxReviews:   0,
		maxRatings:   0,
		booksByDepth: [][]*crawler.Book{},
	}
	processMinMax(graph, &result)
	return result
}

func processMinMax(graph crawler.BookGraph, result *analysis) {
	for _, book := range graph.All {
		if result.minReviews > book.Reviews {
			result.minReviews = book.Reviews
		}
		if result.minRatings > book.Ratings {
			result.minRatings = book.Ratings
		}
		if result.maxReviews < book.Reviews {
			result.maxReviews = book.Reviews
		}
		if result.maxRatings < book.Ratings {
			result.maxRatings = book.Ratings
		}
	}
}
