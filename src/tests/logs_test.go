package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

var testLogManager *logs.Manager

func TestLogManager(t *testing.T) {
	database := db.CreateDB("test_dir_logs", 400, 1)
	testLogManager = database.LogManager;
	fmt.Println("starting test log manager")
	createRecords(1, 35);
	printLogRecords("printing...");
	createRecords(36, 70);
	testLogManager.Flush(70);
	printLogRecords("printing...");
}

func printLogRecords(message string) {
	fmt.Println(message)
	iterator := testLogManager.Iterator()

	for iterator.HasNext() {
		record := iterator.Next()
		newPage := files.LoadBufferToPage(record)
		textStr := newPage.GetString(0)
		nextPosition := files.MaxLengthOfStringOnPage(textStr)
		val := newPage.GetInt(nextPosition)
		fmt.Println("[" + textStr + " , " + strconv.Itoa(val) + "]")
	}
}

func createRecords(start int, end int) {
	fmt.Println("start creating records")

	for i := start; i <= end; i++ {
		record := createLogRecord("record"+strconv.Itoa(i), i+10000)
		lsn := testLogManager.Append(record);
		fmt.Println(lsn);
	}
}

func createLogRecord (str string, num int) []byte {
	nextPosition := files.MaxLengthOfStringOnPage(str);
	byteArr := make([]byte, nextPosition + 4);
	newPage := files.LoadBufferToPage(byteArr);
	newPage.SetString(0, str);
	newPage.SetInt(nextPosition, uint32(num));
	return byteArr;
}
