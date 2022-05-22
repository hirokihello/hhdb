package bufs;

import (
	"testing"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
	"github.com/stretchr/testify/assert"
	"os"
	"time"
)

var bufferManager *Manager;
var fileManager *files.Manager
var logManager *logs.Manager

func TestMain(m *testing.M) {
	fileManager = files.CreateManager("test_dir", 400);
	logManager = logs.CreateManager(fileManager, "test_logfile");
	bufferManager = CreateManager(3, logManager, fileManager);
	m.Run();
	os.RemoveAll("test_dir/*")
}

func TestBuf (t *testing.T) {
	block1 := files.Block{FileName: "test_file", BlockNumber: 1}

	buff1 := bufferManager.Pin(&block1);
	p := buff1.Contents;
	n := p.GetInt(80);
	new_num := int(time.Now().Unix());

	p.SetInt(uint32(new_num), 80);
	buff1.SetModified(1, 0); //placeholder values

	page_1 := files.CreatePage(400);
	fileManager.Read(block1, page_1);

	// // まだ物理ファイルには書き変わっていない。
	assert.Equal(t, page_1.GetInt(80), n);
	assert.NotEqual(t, page_1.GetInt(80), new_num);

	block2 := files.Block{FileName: "test_file", BlockNumber: 2}
	bufferManager.Pin(&block2);
	fileManager.Read(block1, page_1)

	assert.Equal(t, page_1.GetInt(80), n);
	assert.NotEqual(t, page_1.GetInt(80), new_num)

	block3 := files.Block{FileName: "test_file", BlockNumber: 3}
	bufferManager.Pin(&block3);
	fileManager.Read(block1, page_1)

	assert.Equal(t, page_1.GetInt(80), n);
	assert.NotEqual(t, page_1.GetInt(80), new_num)

	bufferManager.Unpin(buff1);
	fileManager.Read(block1, page_1)

	assert.Equal(t, page_1.GetInt(80), n);
	assert.NotEqual(t, page_1.GetInt(80), new_num)

	// 再度ページに対してblock=ファイルの内容を書き込もうとするとすべての変数が置き換わってしまう。意味わからん。
	block4 := files.Block{FileName: "test_file", BlockNumber: 4}
	buff4 := bufferManager.Pin(&block4);
	bufferManager.Unpin(buff4);

	fileManager.Read(block1, page_1);

	assert.NotEqual(t, page_1.GetInt(80), n);
	assert.Equal(t, page_1.GetInt(80), int(new_num))
}
