package recoveries

import (
	"github.com/hirokihello/hhdb/src/files"
)

// 注意
// recovery manager は recovery manger 用のページの領域を確保して操作する
// 最終的に log manager に書き込むが、log manager の内部のページの使い方と recovery manager の page の使い方は異なる
// それぞれの manager で確保されている領域も全く別物になるので注意
// このログ周りで使用する page は、blk のサイズと関係なく、一対一で対応しているわけではない。

// log record の種類を表す定数
const CHECKPOINT = 0
const START = 1
const COMMIT = 2
const ROLLBACK = 3
const SETINT = 4
const SETSTRING = 5

type LogRecord interface {
	Op() int
	TxNumber() int
}

func CreateLogRecord(bytes []byte) LogRecord {
	p := files.CreatePageByBytes(bytes)

	switch p.GetInt(0) {
	case CHECKPOINT:
		return CreateCheckpointRecord()
	case START:
		return CreateStartRecord(p)
	case COMMIT:
		return CreateCommitRecord(p)
	case ROLLBACK:
		return CreateRollbackRecord(p)
	case SETINT:
		return CreateSetIntRecord(p)
	case SETSTRING:
		return CreateSetStringRecord(p)
	default:
		return nil
	}
}
