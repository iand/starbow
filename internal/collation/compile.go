package collation

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
		size := m.Size()
		coll.writers = append(coll.writers, anyWriter{
			Low:  pos,
			High: pos + size,
			Fn:   m.RowWriter(),
		})
		pos += size
		coll.size += size
	}

	coll.contWriters = make(map[string][]contWriter)
	coll.discWriters = make(map[string][]discWriter)
	for _, ms := range s.Measures {
		// Special handling for count, mean, sum, variance - one summary can writing them all at once
		stat64 := false

	measureloop:
		for _, m := range ms.Measures {
			size := m.Size()

			if cm, ok := m.(ContinuousMeasure); ok {
				switch m.(type) {
				case Count, Sum, Mean, Variance:
					if stat64 {
						// We already have a stat64 measure, so reuse it
						continue measureloop
					}
					stat64 = true
				}

				ws := coll.contWriters[ms.Field.Pattern]
				ws = append(ws, contWriter{
					Low:  pos,
					High: pos + size,
					Fn:   cm.ContWriter(),
				})
				coll.contWriters[ms.Field.Pattern] = ws
				pos += size
				coll.size += size
			}

			if dm, ok := m.(DiscreteMeasure); ok {
				ws := coll.discWriters[ms.Field.Pattern]
				ws = append(ws, discWriter{
					Low:  pos,
					High: pos + size,
					Fn:   dm.DiscWriter(),
				})
				coll.discWriters[ms.Field.Pattern] = ws
				pos += size
				coll.size += size
			}

		}
	}

	return coll, nil
}
