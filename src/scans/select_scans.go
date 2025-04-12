package scans

import "github.com/hirokihello/hhdb/src/records"

type SelectScan struct {
	UpdateScan
	scan       Scan
	predicates Predicate
}

func CreateSelectScan(
	scan Scan,
	predicates Predicate,
) *SelectScan {
	return &SelectScan{
		scan:       scan,
		predicates: predicates,
	}
}

func (s *SelectScan) BeforeFirst() {
	s.BeforeFirst()
}

func (s *SelectScan) Next() bool {
	for s.scan.Next() {
		if s.predicates.isSatisfied(s.scan) {
			return true
		}
	}
	return false
}

func (s *SelectScan) GetInt(fieldName string) int {
	return s.scan.GetInt(fieldName)
}
func (s *SelectScan) GetString(fieldName string) string {
	return s.scan.GetString(fieldName)
}
func (s *SelectScan) getValue(fieldName string) any {
	return s.scan.getValue(fieldName)
}
func (s *SelectScan) HasField(fieldName string) bool {
	return s.scan.HasField(fieldName)
}
func (s *SelectScan) Close() {
	s.scan.Close()
}
func (s *SelectScan) SetInt(fieldName string, val int) {
	us := s.scan.(UpdateScan)
	us.SetInt(fieldName, val)
}

func (s *SelectScan) SetString(fieldName string, val string) {
	us := s.scan.(UpdateScan)
	us.SetString(fieldName, val)
}
func (s *SelectScan) SetValue(fieldName string, val any) {
	us := s.scan.(UpdateScan)
	us.SetValue(fieldName, val)
}
func (s *SelectScan) Insert() {
	us := s.scan.(UpdateScan)
	us.Insert()
}
func (s *SelectScan) Delete() {
	us := s.scan.(UpdateScan)
	us.Delete()
}
func (s *SelectScan) GetRid() *records.Rid {
	us := s.scan.(UpdateScan)
	return us.GetRid()
}
func (s *SelectScan) MoveToRid(rid *records.Rid) {
	us := s.scan.(UpdateScan)
	us.MoveToRid(rid)
}
