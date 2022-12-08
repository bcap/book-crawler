package neo4j

import (
	"fmt"
)

type ErrQuery struct {
	query string
	err   error
}

func NewErrQuery(query string, err error) *ErrQuery {
	return &ErrQuery{query: query, err: err}
}

func (e *ErrQuery) Error() string {
	return fmt.Sprintf("failed to execute neo4j query %q: %s", e.query, e.err)
}

func (e *ErrQuery) Unwrap() error {
	return e.err
}
