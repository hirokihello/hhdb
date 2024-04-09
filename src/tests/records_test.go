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
	database := db.CreateDB("test_dir", 400, 3)
	tx := database.NewTransaction()
	schema := records.CreateSchema()

	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	layout := records.CreateLayout(schema)

	for field := range layout.Schema().Fields() {
		offset := layout.Offset(field)
		fmt.Print(field + " has offset " + strconv.Itoa(offset))
	}

	blk := tx.Append("testfile")
	tx.Pin(blk)
	rp = records.CreateRecordPage(tx, blk, layout)
	rp.Format()

	fmt.Print("filling the page with random records")

	slot := rp.InsetAfter(-1)
	for slot > 0 {
		n := rand.Intn(50)
		rp.SetInt(slot, "A", n)
		rp.SetString(slot, "B", "rec"+ strconv.Itoa(n))

		fmt.Print("inserting into slot %d {%d,%d n}", slot, n,n)
		slot = rp.InsetAfter(slot)
	}

	fmt.Print("deleted these records with A-values<25")

	count := 0
	slot = rp.NextAfter(-1)

	for slot > 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")

		if(a < 25) {
			count++
			fmt.Print("slot %d : { %d , %s }", slot, a, b)
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}

	fmt.Print("%d values under 25 were deleted \n", count)
	fmt.Print("here are	the remaing records")
	slot = rp.NextAfter(-1)
	for slot > 0 {
		a := rp.GetInt(slot, "A")
		b := rp.GetString(slot, "B")
		fmt.Print("slot %d : { %d , %s}", slot, a, b)
		slot = rp.NextAfter(slot)
	}

	tx.Unpin(blk)
	tx.Commit()
}
