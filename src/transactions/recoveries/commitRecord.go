package recoveries

import (
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type CommitLogRecord struct {
	LogRecord
	txnum int
}

func (commitRecord CommitLogRecord) Op() int {
	return COMMIT
}

func (commitRecord CommitLogRecord) TxNumber() int {
	return commitRecord.txnum
}

func (commitRecord CommitLogRecord) Undo() {}

func (commitRecord CommitLogRecord) ToString() string {
	return "<COMMIT " + string(commitRecord.txnum) + ">"
}

func CommitRecordWriteToLog(lm logs.Manager, txnum int) int {
	rec := make([]byte, db.INTEGER_BYTES*2)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(db.INTEGER_BYTES, uint32(txnum))
	return lm.Append(rec)
}

func CreateCommitRecord(page files.Page) CommitLogRecord {
	tpos := db.INTEGER_BYTES
	return CommitLogRecord{
		txnum: page.GetInt(tpos),
	}
}
