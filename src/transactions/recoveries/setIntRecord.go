package recoveries

import (
	"strconv"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
	"github.com/hirokihello/hhdb/src/transactionInterface"
)

type SetIntLogRecord struct {
	LogRecord
	txnum  int
	offset int
	blk    files.Block
	val    int
}

func CreateSetIntRecord(p files.Page) SetIntLogRecord {
	// recovery manager の page の使い方に則る。

	// 4 byte 目に格納されている transaction number を取得する
	// 最初の 4 byte にはそのレコードの種類が格納されているため、その次に transaction id が必要になる
	tpos := db.INTEGER_BYTES
	txnum := p.GetInt(tpos)

	// 次に格納されているのはファイル名
	fpos := tpos + db.INTEGER_BYTES
	filename := p.GetString(fpos)

	// 次に格納されているのは、block 情報
	bpos := fpos + files.MaxLengthOfStringOnPage(filename)
	blknum := p.GetInt(bpos)
	blk := files.Block{FileName: filename, Number: blknum}

	// 次に格納されているのは、どんな種類のログレコードかを表す数値
	ops := bpos + db.INTEGER_BYTES
	offset := p.GetInt(ops)

	// 最後にログの内容の string
	vpos := ops + db.INTEGER_BYTES
	val := p.GetInt(vpos)

	return SetIntLogRecord{
		txnum:  txnum,
		offset: offset,
		blk:    blk,
		val:    val,
	}
}

func (setIntLogRecord SetIntLogRecord) Op() int {
	return SETINT
}

func (setIntLogRecord SetIntLogRecord) Txnumber() int {
	return setIntLogRecord.txnum
}

func (setIntLogRecord SetIntLogRecord) ToString() string {
	return "<SETINT " +
		strconv.Itoa(setIntLogRecord.txnum) +
		" " +
		setIntLogRecord.blk.ToString() +
		" " +
		strconv.Itoa(setIntLogRecord.offset) +
		" " +
		strconv.Itoa(setIntLogRecord.val) +
		">"
}


func (setIntLogRecord SetIntLogRecord) UnDo(transaction transactionInterface.TransactionInterface) {
	transaction.Pin(setIntLogRecord.blk)
	// 記録されているのが古い value なので、それを transaction のブロックにセットし直す
	transaction.SetString(setIntLogRecord.blk, setIntLogRecord.offset, setIntLogRecord.val, false)
	transaction.Unpin(setIntLogRecord.blk)
}

func SetIntRecordWriteToLog(lm logs.Manager, txnum int, blk files.Block, offset int, val int) int {
	tpos := db.INTEGER_BYTES
	fpos := tpos + db.INTEGER_BYTES
	bpos := fpos + files.MaxLengthOfStringOnPage(blk.FileName)
	opos := bpos + db.INTEGER_BYTES
	vpos := opos + db.INTEGER_BYTES
	reclen := vpos + db.INTEGER_BYTES

	rec := make([]byte, reclen)
	p := files.CreatePageByBytes(rec)

	p.SetInt(0, SETINT)
	p.SetInt(tpos, uint32(txnum))
	p.SetString(fpos, blk.FileName)
	p.SetInt(bpos, uint32(blk.Number))
	p.SetInt(opos, uint32(offset))
	p.SetInt(vpos, uint32(val))

	return lm.Append(p.Contents())
}
