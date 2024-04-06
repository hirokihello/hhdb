package transaction

import (
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type CheckpointLogRecord struct {
	LogRecord
}

func (checkpointLogRecord *CheckpointLogRecord) Op() int {
	return CHECKPOINT
}

func (checkpointLogRecord *CheckpointLogRecord) TxNumber() int {
	return -1
}

func (checkpointLogRecord *CheckpointLogRecord) Undo() {}

func (checkpointLogRecord *CheckpointLogRecord) ToString() string {
	return "<CHECKPOINT>"
}

func (checkpointLogRecord *CheckpointLogRecord) WriteToLog(lm logs.Manager) int {
	rec := make([]byte, db.INTEGER_BYTES)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}

func CreateCheckpointLogRecord() CheckpointLogRecord {
	return CheckpointLogRecord{}
}
