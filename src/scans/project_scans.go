package scans

type ProjectScan struct {
	Scan
	scan Scan
	fieldList []string
}

func CreateProjectScan(
	scan Scan,
	fieldList []string,
) *ProjectScan {
	return &ProjectScan{
		scan:      scan,
		fieldList: fieldList,
	}
}
func (s *ProjectScan) BeforeFirst() {
	s.scan.BeforeFirst()
}
func (s *ProjectScan) Next() bool {
	return s.scan.Next()
}

func (s *ProjectScan) GetInt(fieldName string) int {
	if !s.HasField(fieldName) {
		panic("Field not found in ProjectScan")
	}
	return s.scan.GetInt(fieldName)
}

func (s *ProjectScan) GetString(fieldName string) string {
	if !s.HasField(fieldName) {
		panic("Field not found in ProjectScan")
	}
	return s.scan.GetString(fieldName)
}
func (s *ProjectScan) GetValue(fieldName string) any {
	if !s.HasField(fieldName) {
		panic("Field not found in ProjectScan")
	}
	return s.scan.GetValue(fieldName)
}

func (s *ProjectScan) HasField(fieldName string) bool {
	for _, field := range s.fieldList {
		if field == fieldName {
			return true
		}
	}
	return false
}
func (s *ProjectScan) Close() {
	s.scan.Close()
}