package collation

import (
	"github.com/iand/starbow/internal/summary/stat64"
)

func (s *Schema) Compile() (Collator, error) {
	coll := Collator{
		keys:    make([][]byte, len(s.Keys)),
		writers: make([]anyWriter, 0, len(s.RecordMeasures)+len(s.Measures)),
	}

	for i := range s.Keys {
		coll.keys[i] = []byte(s.Keys[i].Field.Pattern)
	}
	// TODO: sort keys

	pos := 0
	for _, m := range s.RecordMeasures {
		counter, ok := m.(Count)
		if !ok {
			continue
		}
		coll.writers = append(coll.writers, anyWriter{
			Low:  pos,
			High: pos + counter.Size(),
			Fn:   counter.Update,
		})
		pos += counter.Size()
		coll.size += counter.Size()
	}

	coll.contWriters = make(map[string][]contWriter)
	for _, m := range s.Measures {
		ws := coll.contWriters[m.Field.Pattern]
		ws = append(ws, contWriter{
			Low:  pos,
			High: pos + 32,
			Fn: func(buf []byte, v float64) error {
				s := stat64.New(buf)
				s.Update(stat64.Obs(v))
				return nil
			},
		})

		coll.contWriters[m.Field.Pattern] = ws
		pos += 32
		coll.size += 32
	}

	return coll, nil
}
