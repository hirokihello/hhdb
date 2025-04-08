package metadatas

import (
	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

var MAX_NAME = 16
var TABLE_NAME = "tableName"
var SLOT_SIZE = "slotSize"
var TABLE_CATALOG = "tableCatalog"
var FIELD_CATALOG = "fieldCatalog"

type TableManger struct {
	tableCatalogLayout *records.Layout
	fieldCatalogLayout *records.Layout
}

func CreateTableManager(isNew bool, transaction *transactions.Transaction) *TableManger {
	tableCatalogSchema := records.CreateSchema()
	tableCatalogSchema.AddStringField(TABLE_NAME, MAX_NAME)
	tableCatalogSchema.AddIntField(SLOT_SIZE)
	tableCatalogLayout := records.CreateLayout(tableCatalogSchema)

	fieldCatalogSchema := records.CreateSchema()
	fieldCatalogSchema.AddStringField(TABLE_NAME, MAX_NAME)
	fieldCatalogSchema.AddStringField("fieldName", MAX_NAME)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	fieldCatalogLayout := records.CreateLayout(fieldCatalogSchema)

	tableManager := TableManger{
		tableCatalogLayout: tableCatalogLayout,
		fieldCatalogLayout: fieldCatalogLayout,
	}

	if isNew {
		tableManager.CreateTable(TABLE_CATALOG, tableCatalogSchema, transaction)
		tableManager.CreateTable(FIELD_CATALOG, fieldCatalogSchema, transaction)
	}
	return &tableManager
}

func (t *TableManger) CreateTable(
	tableName string,
	schema *records.Schema,
	transaction *transactions.Transaction,
) {
	layout := records.CreateLayout(schema)

	tableCatalog := records.CreateTableScan(transaction, TABLE_CATALOG, t.tableCatalogLayout)
	tableCatalog.Insert()
	tableCatalog.SetString(TABLE_NAME, tableName)
	tableCatalog.SetInt(SLOT_SIZE, layout.SlotSize())
	tableCatalog.Close()

	fieldCatalog := records.CreateTableScan(transaction, FIELD_CATALOG, t.fieldCatalogLayout)

	for _, fieldName := range schema.Fields() {
		fieldCatalog.Insert()
		fieldCatalog.SetString(TABLE_NAME, tableName)
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
	tableCatalog := records.CreateTableScan(transaction, TABLE_CATALOG, t.tableCatalogLayout)

	for tableCatalog.Next() {
		if tableCatalog.GetString(TABLE_NAME) == tableName {
			size = tableCatalog.GetInt(SLOT_SIZE)
			break
		}
	}
	tableCatalog.Close()

	schema := records.CreateSchema()
	offsets := make(map[string]int)
	fieldCatalog := records.CreateTableScan(
		transaction,
		FIELD_CATALOG,
		t.fieldCatalogLayout,
	)

	for fieldCatalog.Next() {
		if fieldCatalog.GetString(TABLE_NAME) == tableName {
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
