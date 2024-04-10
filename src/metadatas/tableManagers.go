package metadatas

import (
	"fmt"

	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

const MAX_NAME = 16

type TableManager struct {
	tableCatalogLayout *records.Layout
	fieldCatalogLayout *records.Layout
}

func CreateTableManager(isNew bool, transaction *transactions.Transaction) *TableManager {
	tableCatalogSchema := records.CreateSchema()
	tableCatalogSchema.AddStringField("tblname", MAX_NAME)
	tableCatalogSchema.AddIntField("slotsize")
	tableCatalogLayout := records.CreateLayout(tableCatalogSchema)

	fieldCatalogSchema := records.CreateSchema()
	fieldCatalogSchema.AddStringField("tblname", MAX_NAME)
	fieldCatalogSchema.AddStringField("fldname", MAX_NAME)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	fieldCatalogLayout := records.CreateLayout(fieldCatalogSchema)

	tableManager := TableManager{
		tableCatalogLayout: tableCatalogLayout,
		fieldCatalogLayout: fieldCatalogLayout,
	}

	if isNew {
		tableManager.CreateTable("tblcat", tableCatalogSchema, transaction)
		tableManager.CreateTable("fldcat", fieldCatalogSchema, transaction)
	}

	return &tableManager
}

func (tableManager *TableManager) CreateTable(tableName string, tableSchema *records.Schema, transaction *transactions.Transaction) {
	layout := records.CreateLayout(tableSchema)
	tableCatalog := records.CreateTableScan(transaction, "tblcat", tableManager.tableCatalogLayout)
	tableCatalog.Insert()
	tableCatalog.SetString("tblname", tableName)
	tableCatalog.SetInt("slotsize", layout.SlotSize())
	tableCatalog.Close()

	fieldCatalog := records.CreateTableScan(transaction, "fldcat", tableManager.fieldCatalogLayout)
	for fieldName := range tableSchema.Fields() {
		fieldCatalog.Insert()
		fieldCatalog.SetString("tblname", tableName)
		fieldCatalog.SetString("fldname", fieldName)
		fieldCatalog.SetInt("type", tableSchema.Type(fieldName))
		fieldCatalog.SetInt("length", tableSchema.Length(fieldName))
		fieldCatalog.SetInt("offset", layout.Offset(fieldName))
	}

	fieldCatalog.Close()
}

func (tableManager *TableManager) GetLayout(tableName string, transaction *transactions.Transaction) *records.Layout {
	size := -1
	tableCatalog := records.CreateTableScan(transaction, "tblcat", tableManager.tableCatalogLayout)

	for tableCatalog.Next() {
		if tableCatalog.GetString("tblname") == tableName {
			size = tableCatalog.GetInt("slotsize")
			break
		}
	}

	schema := records.CreateSchema()
	offsets := make(map[string]int)
	fieldCatalog := records.CreateTableScan(transaction, "fldcat", tableManager.fieldCatalogLayout)

	for fieldCatalog.Next() {
		if fieldCatalog.GetString("tblname") == tableName {
			fieldName := fieldCatalog.GetString("fldname")
			fieldType := fieldCatalog.GetInt("type")
			fieldLength := fieldCatalog.GetInt("length")
			fieldOffset := fieldCatalog.GetInt("offset")
			offsets[fieldName] = fieldOffset
			schema.AddField(fieldName, fieldType, fieldLength)
		}
	}
	fmt.Println("schema of GetLayout", schema)
	fieldCatalog.Close()

	return records.CreateLayoutByLoadingData(schema, offsets, size)
}
