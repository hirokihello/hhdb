package tests

import (
	"fmt"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/metadatas"
	"github.com/hirokihello/hhdb/src/records"
)

func CatalogTest(t *testing.T) {
	db := db.CreateDB("catalogTest", 400, 8)
	transaction := db.CreateNewTransaction()
	tableManager := metadatas.CreateTableManager(true, transaction)

	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	tableManager.CreateTable("MyTable", schema, transaction)

	fmt.Println("All tables and their lengths:")
	layout := tableManager.GetLayout("tableCatalog", transaction)
	tableScan := records.CreateTableScan(transaction, "tableCatalog", layout)

	for tableScan.Next() {
		tableName := tableScan.GetString("tableName")
		size := tableScan.GetInt("slotSize")
		fmt.Println(tableName, " ", size)
	}

	tableScan.Close()

	fmt.Println("All fields and their offsets:")

	layout2 := tableManager.GetLayout("fieldCatalog", transaction)
	tableScan2 := records.CreateTableScan(transaction, "fieldCatalog", layout2)
	for tableScan2.Next() {
		tableName := tableScan2.GetString("tableName")
		fieldName := tableScan2.GetString("fieldName")
		offset := tableScan2.GetInt("offset")
		fmt.Println(tableName, " ", fieldName, " ", offset)
	}
	tableScan.Close()
}
