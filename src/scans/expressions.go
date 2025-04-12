package scans

import (
	"github.com/hirokihello/hhdb/src/queries"
	"github.com/hirokihello/hhdb/src/records"
)

type Expression struct {
	value     *queries.Constants
	fieldName string
}

func CreateExpression(value *queries.Constants) *Expression {
	return &Expression{
		value: value,
	}
}

func CreateExpressionByFieldName(fieldName string) *Expression {
	return &Expression{
		fieldName: fieldName,
	}
}

// AsFieldName と同じに一旦している。どこで使っているのか次第
func (e *Expression) IsFieldName() bool {
	return &e.fieldName != nil
}

func (e *Expression) AsConstant() *queries.Constants {
	return e.value
}

func (e *Expression) AsFieldName() string {
	return e.fieldName
}

func (e *Expression) Evaluate(s Scan) *queries.Constants {
	if (e.value != nil) {
		return e.value
	}

	return s.GetValue(e.fieldName).(*queries.Constants)
}

func (e *Expression) AppliesTo(schema *records.Schema) bool {
	if (e.value != nil) {
		return true
	}

	return schema.HasField(e.fieldName)
}

func (e *Expression) ToString() string {
	if (e.value != nil) {
		return e.value.ToString()
	}

	return e.fieldName
}