package book

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

func FromNeo4jNode(node *dbtype.Node) *Book {
	value := func(key string, defaultValue any) any {
		if v, has := node.Props[key]; has {
			return v
		}
		return defaultValue
	}
	return &Book{
		Title:        value("title", "").(string),
		Author:       value("author", "").(string),
		Rating:       float32(value("rating", 0.0).(float64)),
		RatingsTotal: int32(value("ratings", 0).(int64)),
		Reviews:      int32(value("reviews", 0).(int64)),
		URL:          value("url", "").(string),
		AlsoRead:     []*Book{},
	}
}

func (b *Book) ToNeo4jAttributes() map[string]any {
	result := make(map[string]any)
	result["title"] = b.Title
	result["author"] = b.Author
	result["rating"] = b.Rating
	result["ratings"] = b.RatingsTotal
	result["reviews"] = b.Reviews
	result["url"] = b.URL
	return result
}
