package metadatas

import (
	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

type MetadataManager struct {
	tableManager *TableManager
	indexManager *IndexManager
	statManager  *StatManager
	viewManager  *ViewManager
}

func CreateMetadataManager(
	isNew bool,
	transaction *transactions.Transaction,
) *MetadataManager {
	tableManager := CreateTableManager(isNew, transaction)
	statManager := CreateStatManager(tableManager, transaction)
	viewManager := CreateViewManager(isNew, tableManager, transaction)
	indexManager := CreateIndexManager(isNew, tableManager, statManager, transaction)

	metadataManager := MetadataManager{
		tableManager: tableManager,
		indexManager: indexManager,
		statManager:  statManager,
		viewManager:  viewManager,
	}

	return &metadataManager
}

func (metadataManager *MetadataManager) CreateTable(
	tableName string,
	schema *records.Schema,
	transaction *transactions.Transaction,
) {
	metadataManager.tableManager.CreateTable(
		tableName, schema, transaction)
}

func (metadataManager *MetadataManager) GetLayout(
	tableName string,
	transaction *transactions.Transaction,
) *records.Layout {
	return metadataManager.tableManager.GetLayout(
		tableName,
		transaction,
	)
}

func (metadataManager *MetadataManager) CreateView(
	viewName string,
	viewDef string,
	transaction *transactions.Transaction,
) {
	metadataManager.viewManager.CreateView(
		viewName, viewDef, transaction)
}

func (metadataManager *MetadataManager) GetViewDef(
	viewName string,
	transaction *transactions.Transaction,
) string {
	return metadataManager.viewManager.GetViewDef(
		viewName, transaction)
}

func (metadataManager *MetadataManager) CreateIndex(
	indexName string,
	tableName string,
	fieldName string,
	transaction *transactions.Transaction,
) {
	metadataManager.indexManager.CreateIndex(
		indexName, tableName, fieldName, transaction)
}

func (metadataManager *MetadataManager) GetIndexInfo(
	tableName string,
	transaction *transactions.Transaction,
) map[string]IndexInfo {
	return metadataManager.indexManager.GetIndexInfo(
		tableName, transaction)
}

func (metadataManager *MetadataManager) GetStatInfo(
	tableName string,
	layout *records.Layout,
	transaction *transactions.Transaction,
) StatInfo {
	return metadataManager.statManager.GetStatInfo(
		tableName, layout, transaction)
}