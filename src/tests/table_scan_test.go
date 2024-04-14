package tests

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/records"
)

func TestTableScan(t *testing.T) {
	database := db.CreateDB("test_dir_scan_tests", 400, 3)
	tx := database.CreateNewTransaction()
	schema := records.CreateSchema()

	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := records.CreateLayout(schema)

	for field := range layout.Schema().Fields() {
		offset := layout.Offset(field)
		fmt.Print(field + " has offset " + strconv.Itoa(offset) + " \n")
	}

	tableScan := records.CreateTableScan(tx, "T", layout)

	fmt.Println("Filling the table with 50 random records")
	tableScan.BeforeFirst()

	for i := 0; i < 50; i++ {
		tableScan.Insert()
		n := rand.Intn(50)
		tableScan.SetInt("A", n)
		tableScan.SetString("B", "rec"+strconv.Itoa(n))

		fmt.Printf("inserting into slot %v {%d,rec %d} \n", tableScan.GetRid(), n, n)
	}

	fmt.Println("Deleting records with A-values < 25")

	count := 0
	tableScan.BeforeFirst()
	for tableScan.Next() {
		a := tableScan.GetInt("A")
		b := tableScan.GetString("B")
		if a < 25 {
			count++
			fmt.Printf("deleted slot %d {%d, %s} \n", tableScan.GetRid(), a, b)
			tableScan.Delete()
		}
	}
	fmt.Printf("%d values under 25 were deleted.\n", count)

	fmt.Println("Here are the remaining records.")
	tableScan.BeforeFirst()

	for tableScan.Next() {
		a := tableScan.GetInt("A")
		b := tableScan.GetString("B")
		fmt.Printf("getting slot %d {%d, %s} \n", tableScan.GetRid(), a, b)
	}

	tableScan.Close()
	tx.Commit()
}
