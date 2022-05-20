package logs;

import (
	"testing"
	"strconv"
	"fmt"
);

import "github.com/hirokihello/hhdb/src/files"

var fileManager files.Manager;
var logManager Manager;

func TestMain(m *testing.M) {
	fileManager = files.CreateManager("test_dir", 400);
	logManager = CreateLogManager(&fileManager, "test_logfile");
	m.Run()
}

func showLogRecords () {
	itr := *logManager.Iterator();
	for itr.HasNext() {
		records := itr.Next();
		page := files.LoadBufferToPage(records);
		s := page.GetString(0);
		npos := files.MaxLengthOfStringOnPage(s);
		value := page.GetInt(npos);
		fmt.Println("[" + s + ", " + strconv.Itoa(value) + "]");
	}
}

func createLogRecords (num int) {
	str := "record: " + strconv.Itoa(num);
	size := files.MaxLengthOfStringOnPage(str)
	// stringの長さ+integer
	new_log := make([]byte, size + 4);
	page := files.LoadBufferToPage(new_log)
	page.SetString(str, 0);
	page.SetInt(uint32(num), size);
	logManager.Append(new_log);
}

func TestBlock (t *testing.T) {
	for i := 0; i < 35; i++ {
		createLogRecords(i);
	}
	showLogRecords()
	for i := 35; i < 400; i++ {
		createLogRecords(i);
	}
	logManager.Flush(65);
	showLogRecords();
}