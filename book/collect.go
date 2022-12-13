package book

import (
	"sort"
	"strings"
)

func Collect(root *Book) []*Book {
	bookMap := make(map[*Book]struct{})
	var recurse func(*Book)
	recurse = func(book *Book) {
		bookMap[book] = struct{}{}
		for _, also := range book.AlsoRead {
			if _, has := bookMap[also.To]; !has {
				recurse(also.To)
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
			return strings.Compare(books[i].Title, books[j].Title) < 0
		},
	)
	return books
}

func CollectByDepth(root *Book) [][]*Book {
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
			recurse(relatedBook.To, depth+1)
		}
	}
	recurse(root, 0)

	booksByDepth := make([][]*Book, maxDepth+1)
	for book, depth := range depthMap {
		booksByDepth[depth] = append(booksByDepth[depth], book)
	}
	return booksByDepth
}
