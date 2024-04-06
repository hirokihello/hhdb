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

func (commitLogRecord *CommitLogRecord) Op() int {
	return COMMIT
}

func (commitLogRecord *CommitLogRecord) TxNumber() int {
	return -1
}

func (commitLogRecord *CommitLogRecord) Undo() {}

func (commitLogRecord *CommitLogRecord) ToString() string {
	return "<COMMIT " + string(commitLogRecord.txnum) + ">"
}

func CommitRecordWriteToLog(lm logs.Manager) int {
	rec := make([]byte, db.INTEGER_BYTES*2)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(db.INTEGER_BYTES, uint32(commitLogRecord.txnum))
	return lm.Append(rec)
}

func CreateCommitRecord(page files.Page) CommitLogRecord {
	tpos := db.INTEGER_BYTES
	return CommitLogRecord{
		txnum: page.GetInt(tpos),
	}
}
