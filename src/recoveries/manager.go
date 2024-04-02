package recoveries

import (
	"github.com/hirokihello/hhdb/src/files"
)

var CHECKPOINT = 0
var START = 1
var COMMIT = 2
var ROLLBACK = 3
var SETINT = 4
var SETSTRING = 5

type Manager struct {
}

// ページの内容に応じて、適切なログを生成する
func (m *Manager) CreateLogRecord(buffer []byte) {
	p := files.LoadBufferToPage(buffer)
	status := p.GetInt(0);
	if (status == CHECKPOINT) {
		return New CheckpointRecord;
	}	else if (status == START) {
		return New StartRecord(p);
	} else if (status == COMMIT) {
		return New CommitRecord(p);
	} else if (status == ROLLBACK) {
		return New RollbackRecord(p);
	} else if (status == SETINT) {
		return New SetIntRecord(p);
	} else if (status == SETSTRING) {
		return New SetStringRecord(p);
	} else {
    log.Fatal("invalid log record!!!!");
    log.Fatal(status);
	}
}

