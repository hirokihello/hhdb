package recoveries

import (
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type StartLogRecord struct {
	LogRecord
	txnum int
}

func (commitLogRecord StartLogRecord) Op() int {
	return COMMIT
}

func (commitLogRecord StartLogRecord) TxNumber() int {
	return -1
}

func (commitLogRecord StartLogRecord) Undo() {}

func (commitLogRecord StartLogRecord) ToString() string {
	return "<COMMIT " + string(commitLogRecord.txnum) + ">"
}

func StartRecordWriteToLog(lm logs.Manager, txnum int) int {
	rec := make([]byte, db.INTEGER_BYTES*2)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(db.INTEGER_BYTES, uint32(txnum))
	return lm.Append(rec)
}

func CreateStartRecord(page files.Page) StartLogRecord {
	tpos := db.INTEGER_BYTES
	return StartLogRecord{
		txnum: page.GetInt(tpos),
	}
}
