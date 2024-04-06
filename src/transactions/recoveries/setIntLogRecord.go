package transaction

import (
	"strconv"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
)

type SetIntLogRecord struct {
	LogRecord
	txnum  int
	offset int
	blk    files.Block
	val    string
}

// func CreateSetIntRecord(p files.Page) SetIntLogRecord {
// 	// recovery manager の page の使い方に則る。

// 	// 4 byte 目に格納されている transaction number を取得する
// 	// 最初の 4 byte にはそのレコードの長さが格納されているため、その次に transaction id が必要になる
// 	tpos := db.INTEGER_BYTES
// 	txnum := p.GetInt(tpos)

// 	// 次に格納されているのはファイル名
// 	fpos := tpos + db.INTEGER_BYTES
// 	filename := p.GetString(fpos)

// 	// 次に格納されているのは、block 情報
// 	bpos := fpos + files.MaxLengthOfStringOnPage(filename)
// 	blknum := p.GetInt(bpos)
// 	blk := files.Block{FileName: filename, Number: blknum}

// 	// 次に格納されているのは、どんな種類のログレコードかを表す数値
// 	ops := bpos + db.INTEGER_BYTES
// 	offset := p.GetInt(ops)

// 	// 最後にログの内容の string
// 	vpos := ops + db.INTEGER_BYTES
// 	val := p.GetString(vpos)

// 	return SetIntLogRecord{
// 		txnum:  txnum,
// 		offset: offset,
// 		blk:    blk,
// 		val:    val,
// 	}
// }

func (stringLogRecord SetIntLogRecord) Op() int {
	return SETINT
}

// func (stringLogRecord SetIntLogRecord) Txnumber() int {
// 	return stringLogRecord.txnum
// }

// func (stringLogRecord SetIntLogRecord) ToString() string {
// 	return "<SETSTRING " +
// 		strconv.Itoa(stringLogRecord.txnum) +
// 		" " +
// 		stringLogRecord.blk.ToString() +
// 		" " +
// 		strconv.Itoa(stringLogRecord.offset) +
// 		" " +
// 		stringLogRecord.val +
// 		">"
// }

// func (stringLogRecord SetIntLogRecord) UnDo(transaction Tx) {
// 	Tx.pin()
// 	Tx.SetString(stringLogRecord.blk, stringLogRecord.offset, stringLogRecord.val, false)
// 	Tx.unpin()
// }

// func (stringLogRecord SetIntLogRecord) WriteToLog(lm files.LogManager, txnum int, blk files.Block, offset int, val, int) {
// 	tpos := db.INTEGER_BYTES
// 	fpos := tpos + db.INTEGER_BYTES
// 	bpos := fpos + files.MaxLengthOfStringOnPage(blk.FileName)
// 	opos := bpos + db.INTEGER_BYTES
// 	vpos := opos + db.INTEGER_BYTES
// 	reclen := vpos += files.MaxLengthOfStringOnPage(val)

// 	rec := make([]byte, reclen)
// 	p := files.CreatePageByByte(rec)

// 	p.setInt(0, SETSTRING);
// 	p.setInt(tpos, txnum);
// 	p.setString(fpos, blk.fileName());
// 	p.setInt(bpos, blk.number());
// 	p.setInt(opos, offset);
// 	p.setString(vpos, val);

// 	return lm.Append(p)
// }
