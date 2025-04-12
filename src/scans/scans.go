package scans

type Scan interface {
	BeforeFirst()
	Next() bool
	GetInt(fieldName string) int
	GetString(fieldName string) string
	GetValue(fieldName string) any
	HasField(fieldName string) bool
	Close()
}