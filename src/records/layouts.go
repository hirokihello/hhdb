package records

import (
	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/files"
)

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

// schema の情報から、それぞれのフィールドのオフセット値と全体の slotsize をいい感じに算出、 layout オブジェクトを作って返す
func CreateLayout(schema *Schema) *Layout {
	layout := Layout{
		schema:  schema,
		offsets: make(map[string]int),
	}
	pos := consts.INTEGER_BYTES // space for the empty / inuse flag
	for fieldName := range schema.Fields() {
		layout.offsets[fieldName] = pos
		pos += layout.lengthInBytes(fieldName)
	}
	layout.slotSize = pos
	return &layout
}

// そのフィールドの中身がどれくらいの領域を必要とするか。int なら 4 bytes. varchar なら文字列の長さ + 4 bytes
func (layout *Layout) lengthInBytes(fieldName string) int {
	fieldType := layout.schema.Type(fieldName)
	if fieldType == INTEGER {
		// int の場合は 4 byte
		return consts.INTEGER_BYTES
	} else {
		// つまり varchar の場合

		// その領域の長さ + 4 byte(領域の長さを示す分)
		return files.MaxLength(layout.schema.Length(fieldName))
	}
}

// 保持している schema 情報を扱えるオブジェクトを返却する
func (layout *Layout) Schema() *Schema {
	return layout.schema
}

func (layout *Layout) Offset(fieldName string) int {
	return layout.offsets[fieldName]
}

func (layout *Layout) SlotSize() int {
	return layout.slotSize
}

// すでに情報をプロセス上で持っているときに layout オブジェクトを扱いたいときに使用する
func CreateLayoutByLoadingData(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	layout := Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
	return &layout
}
