package transactionInterface

import "github.com/hirokihello/hhdb/src/files"

type TransactionI interface {
	Pin(blk files.Block)
	UnPin(blk files.Block)
	SetInt(files.Block, int, int, bool)
	SetString(files.Block, int, string, bool)
}
