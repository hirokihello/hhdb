package records

import "github.com/hirokihello/hhdb/src/consts"

type FieldInfo struct {
	fieldType int // 本来は type だけど go の予約後なので命名変更
	length    int
}

type Schema struct {
	fields map[string]int // 本来は slice. 処理が簡単になるのでこっちで実装している
	info   map[string]FieldInfo
}

// schema に field を追加する
// field type は 4 か 12 のマジックナンバーを使用する(元の実装が java のため、java で使用されているものに準拠している)
func (schema *Schema) AddField(fieldName string, fieldType int, length int) {
	schema.fields[fieldName] = 1 // 本来は slice なので処理を変えている
	schema.info[fieldName] = FieldInfo{fieldType: fieldType, length: length}
}

// schema に int の field を追加する
func (schema *Schema) AddIntField(fieldName string) {
	schema.AddField(fieldName, consts.INTEGER, 0)
}

// schema に string の field を追加する[]
func (schema *Schema) AddStringField(fieldName string, length int) {
	schema.AddField(fieldName, consts.VARCHAR, length)
}

func (schema *Schema) Add(fieldName string, s Schema) {
	schema.AddField(fieldName, s.Type(fieldName), s.Length(fieldName))
}

func (schema *Schema) AddAll(s Schema) {
	for fieldName := range s.Fields() {
		schema.Add(fieldName, s)
	}
}

func (schema *Schema) Fields() map[string]int {
	return schema.fields
}

// 保持している field 名一覧に、引数のフィールドがあるか判定
func (schema *Schema) hasField(fieldName string) bool {
	return schema.fields[fieldName] != 0
}

// そのフィールドの種類を返す
func (schema *Schema) Type(fieldName string) int {
	return schema.info[fieldName].fieldType
}

// そのフィールドの長さを返す int -> 4, varchar なら設定された値
func (schema *Schema) Length(fieldName string) int {
	return schema.info[fieldName].length
}

// schema を初期化して返す。特に何もしない。
func CreateSchema() *Schema {
	return &Schema{
		fields: make(map[string]int),
		info:   make(map[string]FieldInfo),
	}
}
