package bufs;

import (
	// "fmt"
	// "strconv"
	"testing"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
	"github.com/stretchr/testify/assert"
)

var bufferManager *Manager;
var fileManager *files.Manager
var logManager *logs.Manager

func TestMain(m *testing.M) {
	fileManager = files.CreateManager("test_dir", 400);
	logManager = logs.CreateManager(fileManager, "test_logfile");
	bufferManager = CreateManager(3, logManager, fileManager);
	m.Run();
}

func TestBuf (t *testing.T) {
	block1 := files.Block{FileName: "test_file", BlockNumber: 1}
	bufferManager.Pin(&block1);
	buff1 := bufferManager.Pin(&block1);
	p := buff1.Contents;
	n := p.GetInt(80);
	p.SetInt(80, n + 1);

	// まだ物理ファイルには書き変わっていない。
	assert.Equal(t, buff1.Contents.GetInt(80), n);
	assert.NotEqual(t, buff1.Contents.GetInt(80), n + 1);
}