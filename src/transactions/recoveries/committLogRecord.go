package transaction

import (
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type CommitLogRecord struct {
	LogRecord
}

func (commitLogRecord *CommitLogRecord) Op() int {
	return CHECKPOINT
}

func (commitLogRecord *CommitLogRecord) TxNumber() int {
	return -1
}

func (commitLogRecord *CommitLogRecord) Undo() {}

func (commitLogRecord *CommitLogRecord) ToString() string {
	return "<CHECKPOINT>"
}

func (commitLogRecord *CommitLogRecord) WriteToLog(lm logs.Manager) int {
	rec := make([]byte, db.INTEGER_BYTES)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}

func CreateCommitLogRecord() CommitLogRecord {
	return CommitLogRecord{}
}
