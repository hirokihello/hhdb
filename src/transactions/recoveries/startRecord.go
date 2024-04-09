package recoveries

import (
	"strconv"

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

func (startRecord StartRecord) UnDo() {}

func (startRecord StartRecord) ToString() string {
	return "<COMMIT " + strconv.Itoa(startRecord.txnum) + ">"
}

// log sequence number を返り値とする(他のメソッドも同様)
func StartRecordWriteToLog(lm logs.Manager, txnum int) int {
	rec := make([]byte, db.INTEGER_BYTES*2)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, COMMIT)
	p.SetInt(db.INTEGER_BYTES, uint32(txnum))
	return lm.Append(rec)
}

// 二つ目の 4 byte 目に txnum が保存されているような page が引数として渡される
func CreateStartRecord(page files.Page) StartRecord {
	tpos := db.INTEGER_BYTES
	return StartRecord{
		txnum: page.GetInt(tpos),
	}
}
