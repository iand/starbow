package collation

func (s *Schema) Compile() (Collator, error) {
	coll := Collator{
		name:        s.Name,
		keys:        make([][]byte, len(s.Keys)),
		writers:     make([]anyWriter, 0, len(s.RecordMeasures)+len(s.Measures)),
		contWriters: make(map[string][]contWriter),
		discWriters: make(map[string][]discWriter),
		contReaders: make(map[string]map[string]contReader),
		discReaders: make(map[string]map[string]discReader),
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

		if cm, ok := m.(ContinuousOutput); ok {
			crs, found := coll.contReaders[RowPseudoField]
			if !found {
				crs = make(map[string]contReader)
			}
			crs[m.Name()] = contReader{
				Low:  pos,
				High: pos + size,
				Fn:   cm.ContReader(),
			}
			coll.contReaders[RowPseudoField] = crs
		}

		if cm, ok := m.(DiscreteOutput); ok {
			crs, found := coll.discReaders[RowPseudoField]
			if !found {
				crs = make(map[string]discReader)
			}
			crs[m.Name()] = discReader{
				Low:  pos,
				High: pos + size,
				Fn:   cm.DiscReader(),
			}
			coll.discReaders[RowPseudoField] = crs
		}

		pos += size
		coll.size += size

	}

	for _, ms := range s.Measures {
		// Special handling for count, mean, sum, variance - one summary can writing them all at once
		stat64 := false
		stat64Low, stat64Hi := 0, 0

		for _, m := range ms.Measures {
			size := m.Size()
			low, hi := pos, pos+size
			skipWriter := false
			if cm, ok := m.(ContinuousInput); ok {
				switch m.(type) {
				case Count, Sum, Mean, Variance:
					if stat64 {
						// We already have a stat64 measure, so reuse it for writing
						low, hi = stat64Low, stat64Hi
						skipWriter = true
						break
					}
					stat64 = true
					stat64Low = pos
					stat64Hi = pos + size
				}

				if !skipWriter {
					ws := coll.contWriters[ms.Field.Pattern]
					ws = append(ws, contWriter{
						Low:  low,
						High: hi,
						Fn:   cm.ContWriter(),
					})
					coll.contWriters[ms.Field.Pattern] = ws
				}

				if cm, ok := m.(ContinuousOutput); ok {
					crs, found := coll.contReaders[ms.Field.Pattern]
					if !found {
						crs = make(map[string]contReader)
					}
					crs[m.Name()] = contReader{
						Low:  low,
						High: hi,
						Fn:   cm.ContReader(),
					}
					coll.contReaders[ms.Field.Pattern] = crs
				}

				if cm, ok := m.(DiscreteOutput); ok {
					crs, found := coll.discReaders[ms.Field.Pattern]
					if !found {
						crs = make(map[string]discReader)
					}
					crs[m.Name()] = discReader{
						Low:  low,
						High: hi,
						Fn:   cm.DiscReader(),
					}
					coll.discReaders[ms.Field.Pattern] = crs
				}

				pos += size
				coll.size += size
			}

			if dm, ok := m.(DiscreteInput); ok {
				ws := coll.discWriters[ms.Field.Pattern]
				ws = append(ws, discWriter{
					Low:  pos,
					High: pos + size,
					Fn:   dm.DiscWriter(),
				})
				coll.discWriters[ms.Field.Pattern] = ws

				if cm, ok := m.(ContinuousOutput); ok {
					crs, found := coll.contReaders[ms.Field.Pattern]
					if !found {
						crs = make(map[string]contReader)
					}
					crs[m.Name()] = contReader{
						Low:  pos,
						High: pos + size,
						Fn:   cm.ContReader(),
					}
					coll.contReaders[ms.Field.Pattern] = crs
				}

				if cm, ok := m.(DiscreteOutput); ok {
					crs, found := coll.discReaders[ms.Field.Pattern]
					if !found {
						crs = make(map[string]discReader)
					}
					crs[m.Name()] = discReader{
						Low:  pos,
						High: pos + size,
						Fn:   cm.DiscReader(),
					}
					coll.discReaders[ms.Field.Pattern] = crs
				}

				pos += size
				coll.size += size
			}

		}
	}

	return coll, nil
}
