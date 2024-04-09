package recoveries

import (
	"fmt"
	"strconv"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
	transactionInterface "github.com/hirokihello/hhdb/src/transactions/interfaces"
)

type SetStringLogRecord struct {
	LogRecord
	txnum  int
	offset int
	blk    files.Block
	val    string
}

func CreateSetStringRecord(p files.Page) SetStringLogRecord {
	// recovery manager の page の使い方に則る。

	// 4 byte 目に格納されている transaction number を取得する
	// 最初の 4 byte にはそのレコードの長さが格納されているため、その次に transaction id が必要になる
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
	val := p.GetString(vpos)

	fmt.Print("\n")
	fmt.Print(txnum)
	fmt.Print("\n")
	fmt.Print(offset)
	fmt.Print("\n")
	fmt.Print(blk)
	fmt.Print("\n")
	fmt.Print(val)
	fmt.Print("\n")
	return SetStringLogRecord{
		txnum:  txnum,
		offset: offset,
		blk:    blk,
		val:    val,
	}
}

func (stringLogRecord SetStringLogRecord) Op() int {
	return SETSTRING
}

func (stringLogRecord SetStringLogRecord) TxNumber() int {
	return stringLogRecord.txnum
}

func (stringLogRecord SetStringLogRecord) ToString() string {
	return "<SETSTRING " +
		strconv.Itoa(stringLogRecord.txnum) +
		" " +
		stringLogRecord.blk.ToString() +
		" " +
		strconv.Itoa(stringLogRecord.offset) +
		" " +
		stringLogRecord.val +
		">"
}

func (stringLogRecord SetStringLogRecord) UnDo(transaction transactionInterface.TransactionI) {
	transaction.Pin(stringLogRecord.blk)
	// 記録されているのが古い value なので、それを transaction のブロックにセットし直す
	transaction.SetString(stringLogRecord.blk, stringLogRecord.offset, stringLogRecord.val, false)
	transaction.UnPin(stringLogRecord.blk)
}

func SetStringRecordWriteToLog(lm logs.Manager, txnum int, blk files.Block, offset int, val string) int {
	tpos := db.INTEGER_BYTES
	fpos := tpos + db.INTEGER_BYTES
	bpos := fpos + files.MaxLengthOfStringOnPage(blk.FileName)
	opos := bpos + db.INTEGER_BYTES
	vpos := opos + db.INTEGER_BYTES
	reclen := vpos + files.MaxLengthOfStringOnPage(val)

	rec := make([]byte, reclen)
	p := files.CreatePageByBytes(rec)

	p.SetInt(0, SETSTRING)             // 最初の 4 bytes に種類
	p.SetInt(tpos, uint32(txnum))      // 次の 4 byte に transaction number
	p.SetString(fpos, blk.FileName)    // 次に操作したファイル名
	p.SetInt(bpos, uint32(blk.Number)) // 4 byte で block 番号
	p.SetInt(opos, uint32(offset))     // 4 byte で ブロックの変更したオフセット
	p.SetString(vpos, val)             // 文字列

	return lm.Append(p.Contents())
}
