package tests

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/records"
)

func TestRecordManager(t *testing.T) {
	database := db.CreateDB("test_dir_record_manager_tests", 400, 3)
	tx := database.CreateNewTransaction()
	schema := records.CreateSchema()

	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := records.CreateLayout(schema)

	for field := range layout.Schema().Fields() {
		offset := layout.Offset(field)
		fmt.Print(field + " has offset " + strconv.Itoa(offset) + " \n")
	}

	blk := tx.Append("record_test_file")
	tx.Pin(*blk)
	rp := records.CreateRecordPage(tx, blk, layout)
	rp.Format()

	fmt.Print("filling the page with random records \n")

	slot := rp.InsertAfter(-1)
	for slot >= 0 {
		n := rand.Intn(50)
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", "rec"+strconv.Itoa(n))

		fmt.Printf("inserting into slot %d {%d,rec %d} \n", slot, n, n)
		slot = rp.InsertAfter(slot)
	}

	fmt.Print("delete these records with A-values<25 \n")

	count := 0
	slot = rp.NextAfter(-1)

	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")

		if a < 25 {
			count++
			fmt.Printf("slot %d : { %d , %s } \n", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}

	fmt.Printf("%d values under 25 were deleted \n", count)
	fmt.Print("here are	the remaing records \n")
	slot = rp.NextAfter(-1)
	for slot >= 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		fmt.Printf("slot %d : { %d , %s} \n", slot, a, b)
		slot = rp.NextAfter(slot)
	}

	tx.UnPin(*blk)
	tx.Commit()
}
