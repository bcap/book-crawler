package html

import (
	"strings"
)

func CleanText(text string) string {
	nbsp := string([]byte{194, 160})
	text = strings.ReplaceAll(text, nbsp, " ")
	text = strings.TrimSpace(text)
	return text
}
