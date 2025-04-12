package queries

import "strconv"

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

func (c *Constants) Equals(obj any) bool {
	if c2, ok := obj.(*Constants); ok {
		return c.sval == c2.sval && c.ival == c2.ival
	}
	return false
}

func (c *Constants) CompareTo(constant *Constants) int {
	if c.sval == constant.sval && c.ival == constant.ival {
		return 0
	}
	if c.sval < constant.sval || (c.sval == constant.sval && c.ival < constant.ival) {
		return -1
	}
	return 1
}

// 一旦固定文字列でいく。ちゃんと実装する必要ありそうだったら実装する
func (c *Constants) HashCode() int {
	return 12345
}

func (c *Constants) ToString() string {
	if c.sval != "" {
		return c.sval
	}
	return strconv.Itoa(c.ival)
}
