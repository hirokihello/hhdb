package tests

import (
	"fmt"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/metadatas"
	"github.com/hirokihello/hhdb/src/records"
)

func TestCatalog(t *testing.T) {
	database := db.CreateDB("test_dir_of_catalog", 400, 8)
	transaction := database.CreateNewTransaction()
	tableManager := metadatas.CreateTableManager(true, transaction)

	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	tableManager.CreateTable("MyTable", schema, transaction)
	fmt.Println("All tables and their lengths:")

	layout := tableManager.GetLayout("tblcat", transaction)
	tableScan := records.CreateTableScan(transaction, "tblcat", layout)

	for tableScan.Next() {
		tableName := tableScan.GetString("tblname")
		size := tableScan.GetInt("slotsize")
		fmt.Printf("%s %d \n",tableName, size)
	}

	tableScan.Close()


	fmt.Println("Here are the fields for each table and their offsets")

	fcatalogLayout := tableManager.GetLayout("fldcat", transaction)
	tableScan = records.CreateTableScan(transaction, "fldcat", fcatalogLayout)

	for tableScan.Next() {
		tname := tableScan.GetString("tblname")
		fname := tableScan.GetString("fldname")
		offset := tableScan.GetInt("offset")
		fmt.Printf("%s %s %d \n",tname, fname, offset)
	}
	tableScan.Close()
}
