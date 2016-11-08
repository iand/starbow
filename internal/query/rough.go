package query

import (
	"fmt"
	"regexp"

	"github.com/iand/starbow/internal/collation"
)

var (
	queryRe  = regexp.MustCompile(`(?i)^\s*select\s+(.+)\s+where\s+(.+)\s*$`)
	selectRe = regexp.MustCompile(`([^,\s]+)\(([^,\s]+)\)`)
	whereRe  = regexp.MustCompile(`(\S+)='([^']+)'`)
)

// RoughParse is a quick and dirty query parser that uses regular expressions.
func RoughParse(in string) (collation.Query, error) {
	q := collation.Query{}

	parts := queryRe.FindStringSubmatch(in)
	if len(parts) != 3 {
		return q, fmt.Errorf("query: syntax error; expected query to have format 'select xxx where yyy'")
	}

	fields := map[string][][]byte{}

	selections := selectRe.FindAllStringSubmatch(parts[1], -1)
	for _, s := range selections {
		if len(s) != 3 {
			return q, fmt.Errorf("query: syntax error; expected query to have format 'select xxx where yyy'")
		}
		fields[string(s[2])] = append(fields[string(s[2])], []byte(s[1]))
	}

	for f, vs := range fields {
		q.FieldMeasures = append(q.FieldMeasures, collation.FM{F: []byte(f), M: vs})
	}

	wheres := whereRe.FindAllStringSubmatch(parts[2], -1)
	for _, w := range wheres {
		if len(w) != 3 {
			return q, fmt.Errorf("query: syntax error; expected query to have format 'select xxx where yyy'")
		}
		q.Criteria = append(q.Criteria, collation.FV{F: []byte(w[1]), V: []byte(w[2])})
	}

	return q, nil
}
