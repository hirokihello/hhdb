package logs

// import (
// 	"fmt"
// 	"strconv"
// 	"testing"

// 	"github.com/hirokihello/hhdb/src/files"
// 	"github.com/stretchr/testify/assert"
// )

// var fileManager *files.Manager
// var logManager Manager
// var first int;
// func TestMain(m *testing.M) {
// 	fileManager = files.CreateManager("test_dir", 400)
// 	logManager = *CreateManager(fileManager, "test_logfile")
// 	first = logManager.CurrentBlock.BlockNumber;
// 	m.Run()
// }

// func showLogRecords() {
// 	itr := *logManager.Iterator()
// 	// for i := 0; itr.HasNext() && i < 100; i++ {
// 	for itr.HasNext() {
// 		records := itr.Next()
// 		page := files.LoadBufferToPage(records)
// 		s := page.GetString(0)
// 		npos := files.MaxLengthOfStringOnPage(s)
// 		value := page.GetInt(npos)
// 		fmt.Println("[" + s + ", " + strconv.Itoa(value) + "]")
// 	}
// }

// func createLogRecords(num int) {
// 	str := "record: " + strconv.Itoa(num)
// 	size := files.MaxLengthOfStringOnPage(str)
// 	// stringの長さ+integer
// 	new_log := make([]byte, size+4)
// 	page := files.LoadBufferToPage(new_log)
// 	page.SetString(str, 0)
// 	page.SetInt(uint32(num), size)
// 	logManager.Append(page.Contents())
// }

// func TestLogManager(t *testing.T) {
// 	for i := 0; i < 20; i++ {
// 		createLogRecords(i)
// 	}
// 	assert.Equal(t, logManager.CurrentBlock.BlockNumber, first + 1)
// 	showLogRecords()
// 	for i := 35; i < 70; i++ {
// 		createLogRecords(i);
// 	}
// 	fmt.Println("")
// 	logManager.Flush(65);
// 	showLogRecords();
// }
