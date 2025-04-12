package tests

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/metadatas"
	"github.com/hirokihello/hhdb/src/records"
)

func TestMetadataManager(t *testing.T) {
	db := db.CreateDB("test_dir", 400, 3)
	transaction := db.CreateNewTransaction()
	metadataManager := metadatas.CreateMetadataManager(true, transaction)

	schema := records.CreateSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	metadataManager.CreateTable(
		"MyTable",
		schema,
		transaction,
	)

	layout := metadataManager.GetLayout(
		"MyTable",
		transaction,
	)

	size := layout.SlotSize()
	schema2 := layout.Schema()

	fmt.Println("My Table's slot size: ", size)
	fmt.Println("My Table's schema: ", schema2)

	for _, fieldName := range schema2.Fields() {
		var schemaType string
		if schema2.Type(fieldName) == consts.INTEGER {
			schemaType = "INT"
		} else {
			fieldLength := schema2.Length(fieldName)
			schemaType = "VARCHAR(" + fmt.Sprint(fieldLength) + ")"
		}

		fmt.Println("Field name: ", fieldName, " Type: ", schemaType)
	}

	tableScan := records.CreateTableScan(
		transaction,
		"MyTable",
		layout,
	)
	i := 0
	for i < 50 {
		n := rand.Intn(50)
		tableScan.Insert()
		tableScan.SetInt("A", n)
		tableScan.SetString("B", "rec"+fmt.Sprint(n))
		i++
	}

	statInfo := metadataManager.GetStatInfo(
		"MyTable",
		layout,
		transaction,
	)

	fmt.Println("B(Mytable): ", statInfo.BlockAccessed())
	fmt.Println("R(Mytable): ", statInfo.RecordsOutput())
	fmt.Println("V(Mytable, A): ", statInfo.DistinctValues("A"))
	fmt.Println("V(Mytable, B): ", statInfo.DistinctValues("B"))

	viewDef := "SELECT B FROM MyTable WHERE A = 1"
	metadataManager.CreateView(
		"MyView",
		viewDef,
		transaction,
	)
	v := metadataManager.GetViewDef("MyView", transaction)

	fmt.Println("View name: MyView")
	fmt.Println("View def: ", v)

	metadataManager.CreateIndex("indexA", "MyTable", "A", transaction)
	metadataManager.CreateIndex("indexB", "MyTable", "B", transaction)

	indexMap := metadataManager.GetIndexInfo("MyTable", transaction)

	indexInfoA := indexMap["A"]

	fmt.Println("B(MyTable): ", indexInfoA.BlockAccessed())
	fmt.Println("R(MyTable): ", indexInfoA.RecordsOutput())
	fmt.Println("V(MyTable, A): ", indexInfoA.DistinctValues("A"))
	fmt.Println("V(MyTable, B): ", indexInfoA.DistinctValues("B"))

	indexInfoB := indexMap["B"]

	fmt.Println("B(MyTable): ", indexInfoB.BlockAccessed())
	fmt.Println("R(MyTable): ", indexInfoB.RecordsOutput())
	fmt.Println("V(MyTable, A): ", indexInfoB.DistinctValues("A"))
	fmt.Println("V(MyTable, B): ", indexInfoB.DistinctValues("B"))
}
