package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/metadatas"
	"github.com/hirokihello/hhdb/src/records"
)

func TestTableManager(t *testing.T) {
	database := db.CreateDB("test_dir_of_table_manager", 400, 8)
	transaction := database.CreateNewTransaction()
	tableManager := metadatas.CreateTableManager(true, transaction)

	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	fmt.Println("schema", schema)
	tableManager.CreateTable("MyTable", schema, transaction)
	fmt.Println("done create table")
	layout := tableManager.GetLayout("MyTable", transaction)
	fmt.Println("layout", layout)

	fmt.Println("tableManager.GetLayout(mytable, transaction)")
	size := layout.SlotSize()
	schema2 := layout.Schema()

	fmt.Printf("%+v", schema2)

	fmt.Printf("MyTable has slot size %d \n", size)
	fmt.Println("Its fields are:")

	for fieldName := range schema2.Fields() {
		var t string
		if schema2.Type(fieldName) == records.INTERGER {
			t = "int"
		} else {
			strlren := schema2.Length(fieldName)
			t = "varchar(" + strconv.Itoa(strlren) + ") \n"
		}
		fmt.Print(fieldName + ": " + t + " \n")
	}
	transaction.Commit()
}
