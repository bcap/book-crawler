package dot

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/bcap/book-crawler/book"
)

type recurseFn = func(visited map[*book.Book]struct{}, book *book.Book, depth int)

func PrintBookGraph(graph book.Graph, writer io.Writer) {
	// analysis := analyzeGraph(graph)

	genNodes := func() {
		for depth, books := range graph.ByDepth {
			for _, book := range books {
				label := fmt.Sprintf(
					"%s\\l%s\\l%0.1f (%d ratings)\\l%d reviews\\ldepth:%d\\l",
					book.Title,
					book.Author,
					book.Rating,
					book.RatingsTotal,
					book.Reviews,
					depth,
				)
				fmt.Fprintf(
					writer,
					"%q [nojustify=false label=\"%s\" URL=\"%s\"]\n",
					bookID(book),
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
				nodes[idx] = fmt.Sprintf("\"%s by %s\"", book.Title, book.Author)
			}
			fmt.Fprintf(writer, "{rank=%s; %s}\n", rank, strings.Join(nodes, "; "))
		}
	}

	var genEdges recurseFn
	genEdges = func(visited map[*book.Book]struct{}, book *book.Book, depth int) {
		visited[book] = struct{}{}
		for idx, relatedBook := range book.AlsoRead {
			label := fmt.Sprintf("idx:%d", idx)
			fmt.Fprintf(writer, "%q -> %q [label=%q]\n", bookID(book), bookID(relatedBook.To), label)
		}
		for _, relatedBook := range book.AlsoRead {
			if _, v := visited[relatedBook.To]; !v {
				genEdges(visited, relatedBook.To, depth+1)
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
	genEdges(map[*book.Book]struct{}{}, graph.Root, 0)

	fmt.Fprint(writer, "\n}\n")
}

type analysis struct {
	minReviews int32
	maxReviews int32
	minRatings int32
	maxRatings int32

	booksByDepth [][]*book.Book
}

func analyzeGraph(graph book.Graph) analysis {
	result := analysis{
		minReviews:   math.MaxInt32,
		minRatings:   math.MaxInt32,
		maxReviews:   0,
		maxRatings:   0,
		booksByDepth: [][]*book.Book{},
	}
	processMinMax(graph, &result)
	return result
}

func processMinMax(graph book.Graph, result *analysis) {
	for _, book := range graph.All {
		if result.minReviews > book.Reviews {
			result.minReviews = book.Reviews
		}
		if result.minRatings > book.RatingsTotal {
			result.minRatings = book.RatingsTotal
		}
		if result.maxReviews < book.Reviews {
			result.maxReviews = book.Reviews
		}
		if result.maxRatings < book.RatingsTotal {
			result.maxRatings = book.RatingsTotal
		}
	}
}

func bookID(b *book.Book) string {
	return fmt.Sprintf("%s by %s", b.Title, b.Author)
}
