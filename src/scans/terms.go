package scans

import (
	"github.com/hirokihello/hhdb/src/queries"
	"github.com/hirokihello/hhdb/src/records"
)

type Term struct {
	lhs *Expression
	rhs *Expression
}

func CreateTerm(lhs *Expression, rhs *Expression) *Term {
	return &Term{
		lhs: lhs,
		rhs: rhs,
	}
}

func (t *Term) IsSatisfied(s Scan) bool {
	lhsval := t.lhs.Evaluate(s)
	rhsval := t.rhs.Evaluate(s)
	return rhsval.Equals(lhsval)
}

func (t *Term) AppliesTo(schema *records.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

// 存在すると動かないので、一旦コメントアウト
// func (t *Term) ReductionFactor(p Plan) float64 {
// 	var lhsName, rhsName string
// 	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
// 		lhsName = t.lhs.AsFieldName()
// 		rhsName = t.rhs.AsFieldName()
// 		return math.Max(
// 			p.DistinctValues(lhsName),
// 			p.DistinctValues(rhsName),
// 		)
// 	}

// 	if t.lhs.IsFieldName() {
// 		lhsName = t.lhs.AsFieldName()
// 		return p.DistinctValues(lhsName)
// 	}

// 	if t.rhs.IsFieldName() {
// 		rhsName = t.rhs.AsFieldName()
// 		return p.DistinctValues(rhsName)
// 	}

// 	if t.lhs.AsConstant() == t.rhs.AsConstant() {
// 		return 1.0
// 	} else {
// 		// integer の max
// 		return math.MaxInt
// 	}
// }

func (t *Term) EquatesWithConstant(fieldName string) *queries.Constants {
	if t.lhs.IsFieldName() {
		if t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
			return t.rhs.AsConstant()
		}
	} else if t.rhs.IsFieldName() {
		if t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
			return t.lhs.AsConstant()
		}
	}

	return nil
}

func (t *Term) EquatesWithField(fieldName string) string {
	if t.lhs.IsFieldName() &&
		t.lhs.AsFieldName() == fieldName &&
		t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	} else if t.rhs.IsFieldName() &&
		t.rhs.AsFieldName() == fieldName &&
		t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}

	return ""
}
