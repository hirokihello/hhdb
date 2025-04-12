package scans

import "github.com/hirokihello/hhdb/src/records"

type UpdateScan interface {
	Scan
	SetInt(fieldName string, val int)
	SetString(fieldName string, val string)
	SetValue(fieldName string, val any)
	Insert()
	Delete()
	GetRid() *records.Rid
	MoveToRid(rid *records.Rid)
}
