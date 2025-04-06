package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/metadatas"
	"github.com/hirokihello/hhdb/src/records"
)

func TableManagerTest(t *testing.T) {
	db := db.CreateDB("test_dir_tblManagerTest", 400, 8)
	transaction := db.CreateNewTransaction()
	tableManger := metadatas.CreateTableManager(true, transaction)

	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	tableManger.CreateTable("MyTable", schema, transaction)

	layout := tableManger.GetLayout("MyTable", transaction)
	size := layout.SlotSize()
	schema2 := layout.Schema()

	fmt.Println("MyTable has slot size", size)
	fmt.Println("Its fields are:")
	for _, fieldName := range schema2.Fields() {
		var typeOfField string
		if schema2.Type(fieldName) == consts.INTEGER {
			typeOfField = "int"
		} else {
			strLength := schema2.Length(fieldName)
			typeOfField = "varchar(" + strconv.Itoa(strLength) + ")"
		}

		fmt.Println(fieldName, ": ", typeOfField)
	}

	transaction.Commit()
}
