package transactionInterface

import "github.com/hirokihello/hhdb/src/files"

type TransactionInterface interface {
	Pin(blk files.Block)
	Unpin(blk files.Block)
	SetInt(files.Block, int, int, bool)
	SetString(files.Block, int, string, bool)
}
