package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/records"
)

func TestLayout(t *testing.T) {
	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := records.CreateLayout(schema)

	for _, fieldName := range layout.Schema().Fields() {
		offset := layout.Offset(fieldName)
		// 4 / 8 になる。順番的に、最初の 4 byte. 次に int が 4 byte. 最後に string になるので。
		fmt.Print(fieldName + " has offset: " + strconv.Itoa(offset) + "\n")
	}
}
