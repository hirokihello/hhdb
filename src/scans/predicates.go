package scans

import (
	"github.com/hirokihello/hhdb/src/queries"
	"github.com/hirokihello/hhdb/src/records"
)

type Predicate struct {
	terms []*Term
}

func CreatePredict() *Predicate {
	return &Predicate{
		terms: make([]*Term, 0),
	}
}
func CreatePredictWithTerm(term *Term) *Predicate {
	return &Predicate{
		terms: []*Term{term},
	}
}

func (p *Predicate) ConJoinWith(predicate *Predicate) {
	p.terms = append(p.terms, predicate.terms...)
}

func (p *Predicate) IsSatisfied(scan Scan) bool {
	for _, term := range p.terms {
		if !term.IsSatisfied(scan) {
			return false
		}
	}
	return true
}

// 現在 plan が定義されていないのでコメントアウト
// func (p *Predicate) ReductionFactor(plan Plan) int {
// 	factor := 1;
// 	for term := range p.terms {
// 		factor *= term.ReductionFactor(plan)
// 	}
// 	return factor
// }

func (p *Predicate) SelectSubPred(schema *records.Schema) *Predicate {
	result := CreatePredict()
	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}

	return result
}

func (p *Predicate) JoinSubPredicates(schema1 *records.Schema, schema2 *records.Schema) *Predicate {
	result := CreatePredict()
	newSchema := records.CreateSchema()
	newSchema.AddAll(*schema1)
	newSchema.AddAll(*schema2)

	for _, term := range p.terms {
		if !term.AppliesTo(schema1) &&
			!term.AppliesTo(schema2) &&
			term.AppliesTo(newSchema) {
			result.terms = append(result.terms, term)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}

	return result
}

func (p *Predicate) EquatesWithConstant(fieldName string) *queries.Constants {
	for _, term := range p.terms {
		if value := term.EquatesWithConstant(fieldName); value != nil {
			if value != nil {
				return value
			}
		}
	}
	return nil
}

func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, term := range p.terms {
		if value := term.EquatesWithField(fieldName); value != "" {
			return value
		}
	}
	return ""
}

// これでいいのかは自信が全くないので、デバッグで真っ先に疑うこと
func (p *Predicate) ToString() string {
	if len(p.terms) == 0 {
		return ""
	}
	var result = ""
	for i, term := range p.terms {
		if i == 0 {
			result += term.lhs.ToString() + term.rhs.ToString()
		} else {
			result += " AND " + term.lhs.ToString() + term.rhs.ToString()
		}
	}

	return result
}
