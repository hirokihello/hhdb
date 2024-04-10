package queries

type Constants struct {
	sval string
	ival int
}

func CreateConstantByString(val string) *Constants {
	return &Constants{sval: val}
}

func CreateConstantByInt(val int) *Constants {
	return &Constants{ival: val}
}

func (c *Constants) AsString() string {
	return c.sval
}

func (c *Constants) AsInt() int {
	return c.ival
}