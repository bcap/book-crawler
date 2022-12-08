package book

type Graph struct {
	Root    *Book
	All     []*Book
	ByDepth [][]*Book
}

func NewGraph(root *Book) Graph {
	return Graph{
		Root:    root,
		All:     Collect(root),
		ByDepth: CollectByDepth(root),
	}
}

type Book struct {
	Title string

	Author    string
	AuthorURL string

	Rating       float32
	RatingsTotal int32
	Ratings1     int32
	Ratings2     int32
	Ratings3     int32
	Ratings4     int32
	Ratings5     int32

	Reviews int32

	URL string

	AlsoRead []*Book
}

func New(url string) *Book {
	return &Book{
		URL:      url,
		AlsoRead: make([]*Book, 0),
	}
}
