package tests

import (
	"fmt"
	"strconv"
	"testing"
)

func TestLayout(t *testing.T) {
	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := records.CreateLayout(schema)

	for fieldName := range layout.Schema().Fields() {
		offset := layout.Offset(fieldName)
		fmt.Print(fieldName + " has offset: " + strconv.Atoi(offset))
	}
}
