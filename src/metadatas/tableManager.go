package metadatas

import (
	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

var MAX_NAME = 16

type TableManger struct {
	tableCatalogLayout *records.Layout
	fieldCatalogLayout *records.Layout
}

func CreateTableManager(isNew bool, transaction *transactions.Transaction) *TableManger {
	tableCatalogSchema := records.CreateSchema()
	tableCatalogSchema.AddStringField("tableName", MAX_NAME)
	tableCatalogSchema.AddIntField("slotSize")
	tableCatalogLayout := records.CreateLayout(tableCatalogSchema)

	fieldCatalogSchema := records.CreateSchema()
	fieldCatalogSchema.AddStringField("tableName", MAX_NAME)
	fieldCatalogSchema.AddStringField("fieldName", MAX_NAME)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	fieldCatalogLayout := records.CreateLayout(fieldCatalogSchema)
	tableManager := TableManger{
		tableCatalogLayout: tableCatalogLayout,
		fieldCatalogLayout: fieldCatalogLayout,
	}

	return &tableManager
}

func (t *TableManger) CreateTable(
	tableName string,
	schema *records.Schema,
	transaction *transactions.Transaction,
) {
	layout := records.CreateLayout(schema)

	tableCatalog := records.CreateTableScan(transaction, "tableCatalog", t.tableCatalogLayout)
	tableCatalog.Insert()
	tableCatalog.SetString("tableName", tableName)
	tableCatalog.SetInt("slotSize", layout.SlotSize())
	tableCatalog.Close()

	fieldCatalog := records.CreateTableScan(transaction, "fieldCatalog", t.fieldCatalogLayout)

	for _, fieldName := range schema.Fields() {
		fieldCatalog.Insert()
		fieldCatalog.SetString("tableName", tableName)
		fieldCatalog.SetString("fieldName", fieldName)
		fieldCatalog.SetInt("type", schema.Type(fieldName))
		fieldCatalog.SetInt("length", schema.Length(fieldName))
		fieldCatalog.SetInt("offset", layout.Offset(fieldName))
	}
	fieldCatalog.Close()
}

func (t *TableManger) GetLayout(
	tableName string,
	transaction *transactions.Transaction,
) *records.Layout {
	size := -1
	tableCatalog := records.CreateTableScan(transaction, "tableCatalog", t.tableCatalogLayout)

	for tableCatalog.Next() {
		if tableCatalog.GetString("tableName") == tableName {
			size = tableCatalog.GetInt("slotSize")
			break
		}
	}
	tableCatalog.Close()

	schema := records.CreateSchema()
	var offsets map[string]int
	fieldCatalog := records.CreateTableScan(
		transaction,
		"fieldCatalog",
		t.fieldCatalogLayout,
	)

	for fieldCatalog.Next() {
		if fieldCatalog.GetString("tableName") == tableName {
			fieldName := fieldCatalog.GetString("fieldName")
			fieldType := fieldCatalog.GetInt("type")
			fieldLength := fieldCatalog.GetInt("length")
			offset := fieldCatalog.GetInt("offset")
			offsets[fieldName] = offset
			schema.AddField(fieldName, fieldType, fieldLength)
		}
	}

	fieldCatalog.Close()
	return records.CreateLayoutByLoadingData(schema, offsets, size)
}
