// Package columnrender renders bounded, sanitized terminal columns.
package columnrender

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/toolsupply/ticket-orc/internal/terminaltext"
)

// Render writes rows with aligned columns. A zero width derives the column
// width from all rows; a positive width bounds that column and ellipsizes
// longer values. The final column is not padded.
func Render(out io.Writer, rows [][]string, widths []int, separator string) {
	if len(rows) == 0 {
		return
	}
	columns := len(widths)
	if columns == 0 {
		for _, row := range rows {
			if len(row) > columns {
				columns = len(row)
			}
		}
		widths = make([]int, columns)
	} else {
		widths = append([]int(nil), widths...)
	}
	values := make([][]string, len(rows))
	for i, row := range rows {
		values[i] = make([]string, columns)
		for column := 0; column < columns && column < len(row); column++ {
			value := terminaltext.Sanitize(row[column], false)
			values[i][column] = value
		}
	}
	for column := range widths {
		if widths[column] != 0 {
			continue
		}
		for _, row := range values {
			if column < len(row) {
				if size := utf8.RuneCountInString(row[column]); size > widths[column] {
					widths[column] = size
				}
			}
		}
	}
	for _, row := range values {
		for column, value := range row {
			if column > 0 {
				_, _ = io.WriteString(out, separator)
			}
			if widths[column] > 0 {
				value = ellipsize(value, widths[column])
			}
			_, _ = io.WriteString(out, value)
			if column < len(row)-1 {
				padding := widths[column] - utf8.RuneCountInString(value)
				if padding > 0 {
					_, _ = io.WriteString(out, strings.Repeat(" ", padding))
				}
			}
		}
		_, _ = io.WriteString(out, "\n")
	}
}

func ellipsize(value string, max int) string {
	if max <= 0 || utf8.RuneCountInString(value) <= max {
		return value
	}
	if max <= 3 {
		return string([]rune(value)[:max])
	}
	runes := []rune(value)
	return fmt.Sprintf("%s...", string(runes[:max-3]))
}
