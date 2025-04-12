package metadatas

import (
	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

type IndexManager struct {
	tableManager *TableManager
	layout       *records.Layout
	statManager  *StatManager
}

func CreateIndexManager(
	isNew bool,
	tableManager *TableManager,
	statManager *StatManager,
	transaction *transactions.Transaction,
) *IndexManager {
	if isNew {
		schema := records.CreateSchema()
		schema.AddStringField("indexName", MAX_NAME)
		schema.AddStringField("tableName", MAX_NAME)
		schema.AddStringField("fieldName", MAX_NAME)
		tableManager.CreateTable("indexCatalog", schema, transaction)
	}
	indexManager := IndexManager{
		tableManager: tableManager,
		layout:       tableManager.GetLayout("indexCatalog", transaction),
		statManager:  statManager,
	}
	return &indexManager
}

func (i *IndexManager) CreateIndex(
	indexName string,
	tableName string,
	fieldName string,
	transaction *transactions.Transaction,
) {
	layout := i.layout
	tableScan := records.CreateTableScan(transaction, "indexCatalog", layout)
	tableScan.Insert()
	tableScan.SetString("indexName", indexName)
	tableScan.SetString("tableName", tableName)
	tableScan.SetString("fieldName", fieldName)
	tableScan.Close()
}

func (i *IndexManager) GetIndexInfo(
	tableName string,
	transaction *transactions.Transaction,
) map[string]IndexInfo {
	result := make(map[string]IndexInfo)
	tableScan := records.CreateTableScan(transaction, "indexCatalog", i.layout)
	for tableScan.Next() {
		if tableScan.GetString("tableName") == tableName {
			indexName := tableScan.GetString("indexName")
			fieldName := tableScan.GetString("fieldName")
			layout := i.tableManager.GetLayout(tableName, transaction)
			statInfo := i.statManager.GetStatInfo(tableName, layout, transaction)
			result[fieldName] = CreateIndexInfo(
				indexName,
				fieldName,
				layout.Schema(),
				transaction,
				statInfo,
			)
		}
	}
	tableScan.Close()
	return result
}

type IndexInfo struct {
	indexName   string
	fieldName   string
	transaction *transactions.Transaction
	layout      *records.Layout
	schema      *records.Schema
	statInfo    *StatInfo
}

func CreateIndexInfo(
	indexName string,
	fieldName string,
	schema *records.Schema,
	transaction *transactions.Transaction,
	statInfo StatInfo,
) IndexInfo {
	return IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		transaction: transaction,
		schema:      schema,
		layout:      createIndexLayout(fieldName),
		statInfo:    &statInfo,
	}
}

func createIndexLayout(fieldName string) *records.Layout {
	schema := records.CreateSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	if schema.Type(fieldName) == consts.INTEGER {
		schema.AddIntField("dataValue")
	} else {
		schema.AddStringField("dataValue", schema.Length(fieldName))
	}
	return records.CreateLayout(schema)
}

func (i *IndexInfo) Open() {
	// return CreateBTreeIndex(
	// i.transaction,
	// i.indexName,
	// i.layout
	// )
}

func (i *IndexInfo) BlockAccessed() int {
	rpb := i.transaction.BlockSize() / i.layout.SlotSize()
	numBlocks := i.statInfo.RecordsOutput() / rpb
	// return hashIndex.searchCost(numBlocks, rpb)
	return numBlocks
}

func (i *IndexInfo) RecordsOutput() int {
	return i.statInfo.RecordsOutput()
}
func (i *IndexInfo) DistinctValues(fieldName string) int {
	if fieldName == i.fieldName {
		return 1
	} else {
		return i.statInfo.DistinctValues(fieldName)
	}
}
