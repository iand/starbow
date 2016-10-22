package collation

// A Schema defines the structure of a collation.
type Schema struct {
	Name     string
	Keys     []KeySpec
	Measures []MeasureSpec
}

// A KeySpec specifies a key field in a schema.
type KeySpec struct {
	Field FieldSpec
}

// A FieldSpec specifies a field from an incoming observation.
type FieldSpec struct {
	Pattern string
}

// A MeasureSpec specifies a measure field in a schema.
type MeasureSpec struct {
	Field    FieldSpec
	Measures Measures
}
