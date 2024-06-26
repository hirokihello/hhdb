package recoveries

import (
	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type RollbackRecord struct {
	LogRecord
	txnum int
}

func (rollbackRecord RollbackRecord) Op() int {
	return ROLLBACK
}

func (rollbackRecord RollbackRecord) TxNumber() int {
	return rollbackRecord.txnum
}

func (rollbackRecord RollbackRecord) UnDo() {}

func (rollbackRecord RollbackRecord) ToString() string {
	return "<CHECKPOINT>"
}

// 二つ目の 4 byte 目に txnum が保存されているような page が引数として渡される
func CreateRollbackRecord(page files.Page) RollbackRecord {
	return RollbackRecord{
		txnum: page.GetInt(consts.INTEGER_BYTES),
	}
}

// public に呼び出せる
func RollbackRecordWriteToLog(lm logs.Manager, txnum int) int {
	rec := make([]byte, consts.INTEGER_BYTES*2)
	p := files.CreatePageByBytes(rec)
	p.SetInt(0, ROLLBACK)
	p.SetInt(consts.INTEGER_BYTES, uint32(txnum))
	return lm.Append(rec)
}
