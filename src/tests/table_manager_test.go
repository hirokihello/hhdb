package tests

import (
	"fmt"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
)

func TestTableManager(t *testing.T) {
	database := db.CreateDB("test_dir_of_table_manager", 400, 8)
	transaction := database.CreateNewTransaction()

}
