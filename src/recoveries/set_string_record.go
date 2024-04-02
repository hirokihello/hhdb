package recoveries

import "github.com/hirokihello/hhdb/src/files"

type SetStringRecord struct {
	txnum  int
	offset int
	val    string
	blk    files.Block
}

func CreateSetStringRecord(p files.Page) {
	tpos := 4 // 3
	txnum = p.GetInt(tpos)
}
