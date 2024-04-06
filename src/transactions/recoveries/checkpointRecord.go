package recoveries

import (
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type CheckpointLogRecord struct {
	LogRecord
}

func (checkpointLogRecord CheckpointLogRecord) Op() int {
	return CHECKPOINT
}

func (checkpointLogRecord CheckpointLogRecord) TxNumber() int {
	return -1
}

func (checkpointLogRecord CheckpointLogRecord) Undo() {}

func (checkpointLogRecord CheckpointLogRecord) ToString() string {
	return "<CHECKPOINT>"
}

func CheckpointRecordWriteToLog(lm logs.Manager) int {
	rec := make([]byte, db.INTEGER_BYTES)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}

// 二つ目の 4 byte 目に txnum が保存されているような page が引数として渡される
func CreateCheckpointRecord() CheckpointLogRecord {
	return CheckpointLogRecord{}
}
