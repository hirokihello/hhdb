package metadatas

import (
	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

type ViewManager struct {
	TableManger *TableManger
}

const MAX_VIEW_DEF = 100

func (v *ViewManager) CreateView(
	viewName string,
	viewDef string,
	transaction *transactions.Transaction,
) {
	layout := v.TableManger.GetLayout("viewCatalog", transaction)
	tableScan := records.CreateTableScan(transaction, "viewCatalog", layout)
	// この行いらないかも？
	tableScan.Insert()
	tableScan.SetString("viewName", viewName)
	tableScan.SetString("viewDef", viewDef)
	tableScan.Close()
}

func (v *ViewManager) GetViewDef(
	viewName string,
	transaction *transactions.Transaction,
) string {
	var result string
	layout := v.TableManger.GetLayout("viewCatalog", transaction)
	tableScan := records.CreateTableScan(transaction, "viewCatalog", layout)
	tableScan.SetString("viewName", viewName)

	for tableScan.Next() {
		if tableScan.GetString("viewName") == viewName {
			result = tableScan.GetString("viewDef")
		}
	}
	tableScan.Close()
	return result
}

func CreateViewManager(
	isNew bool,
	tableManager *TableManger,
	transaction *transactions.Transaction,
) *ViewManager {
	viewManager := ViewManager{
		TableManger: tableManager,
	}
	if isNew {
		schema := records.CreateSchema()
		schema.AddStringField("viewName", MAX_NAME)
		schema.AddStringField("viewDef", MAX_VIEW_DEF)
		tableManager.CreateTable("viewCatalog", schema, transaction)
	}
	return &viewManager
}
