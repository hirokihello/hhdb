package buffers;

import (
	// "fmt"
	// "strconv"
	"testing"
	"github.com/hirokihello/hhdb/src/engine";
	"github.com/hirokihello/hhdb/src/files"
	"github.com/stretchr/testify/assert"
)

var db *engine.Db;
var bufferManager *Manager;

func TestMain(m *testing.M) {
	db = engine.CreateDb("test_dir", 400, 3);
	bufferManager = *db.BufferManager;
	m.Run()
}

func TestBuf (t *testing.T) {
	block1 := files.Block{FileName: "test_file", BlockNumber: 1}
	buff1 := bufferManager.Pin(&block1);
	p := buff1.Contents;
	n := p.GetInt(80);
	p.SetInt(80, n + 1);

	assert.Equal(t, buff1.Contents.GetInt(80), n);
}