package scans

type ProductScan struct {
	Scan
	scan1      Scan
	scan2      Scan
}

func CreateProductScan(
	scan1 Scan,
	scan2 Scan,
) *ProductScan {
	return &ProductScan{
		scan1: scan1,
		scan2: scan2,
	}
}
func (s *ProductScan) BeforeFirst() {
	s.scan1.BeforeFirst()
	// この部分いる？？
	s.scan1.Next()
	s.scan2.BeforeFirst()
}

func (s *ProductScan) Next() bool {
	if !s.scan1.Next() {
		return false
	}
	s.scan2.BeforeFirst()
	s.scan2.Next()
	return true
}
func (s *ProductScan) GetInt(fieldName string) int {
	if s.scan1.HasField(fieldName) {
		return s.scan1.GetInt(fieldName)
	}
	return s.scan2.GetInt(fieldName)
}
func (s *ProductScan) GetString(fieldName string) string {
	if s.scan1.HasField(fieldName) {
		return s.scan1.GetString(fieldName)
	}
	return s.scan2.GetString(fieldName)
}
func (s *ProductScan) GetValue(fieldName string) any {
	if s.scan1.HasField(fieldName) {
		return s.scan1.GetValue(fieldName)
	}
	return s.scan2.GetValue(fieldName)
}
func (s *ProductScan) HasField(fieldName string) bool {
	if s.scan1.HasField(fieldName) {
		return true
	}
	return s.scan2.HasField(fieldName)
}
func (s *ProductScan) Close() {
	s.scan1.Close()
	s.scan2.Close()
}