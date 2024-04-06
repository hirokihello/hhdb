package recoveries

import (
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type StartRecord struct {
	LogRecord
	txnum int
}

func (startRecord StartRecord) Op() int {
	return COMMIT
}

func (startRecord StartRecord) TxNumber() int {
	return startRecord.txnum
}

func (startRecord StartRecord) Undo() {}

func (startRecord StartRecord) ToString() string {
	return "<COMMIT " + string(startRecord.txnum) + ">"
}

func StartRecordWriteToLog(lm logs.Manager, txnum int) int {
	rec := make([]byte, db.INTEGER_BYTES*2)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(db.INTEGER_BYTES, uint32(txnum))
	return lm.Append(rec)
}

func CreateStartRecord(page files.Page) StartRecord {
	tpos := db.INTEGER_BYTES
	return StartRecord{
		txnum: page.GetInt(tpos),
	}
}
