package transactions

import (
	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
	"github.com/hirokihello/hhdb/src/transactions/concurrencies"
	transactionInterface "github.com/hirokihello/hhdb/src/transactions/interfaces"
	"github.com/hirokihello/hhdb/src/transactions/recoveries"
)

const END_OF_FILE int = -1

// プログラム全体で一つの変数を参照したいため。基本使用するときはポインタを参照して使用する
var nextTransactionNum int

type Transaction struct {
	transactionInterface.TransactionInterface
	recoveryManager    *recoveries.RecoveryManager
	concurrencyManager *concurrencies.Manager
	bufferManager      *buffers.Manager
	fileManager        *files.Manager
	txnum              int
	myBuffers          BufferList
}

func CreateTransaction(fileManager files.Manager, logManager logs.Manager, bufferManager buffers.Manager) *Transaction {
	transaction := &Transaction{
		fileManager:        &fileManager,
		bufferManager:      &bufferManager,
		txnum:              nextTxNumber(),
		concurrencyManager: concurrencies.CreateConcurrencyManager(),
		myBuffers:          BufferList{bufferManager: bufferManager},
	}

	recoveryManager := recoveries.CreateRecoveryManager(logManager, bufferManager, transaction, nextTxNumber())
	transaction.recoveryManager = recoveryManager

	return transaction
}

func nextTxNumber() int {
	return -1
}

func (transaction Transaction) Unpin(blk files.Block) {}

func (transaction Transaction)Pin(blk files.Block) {}

func (transaction Transaction)SetInt(blk files.Block, offset int, val int, oktolog bool) {}

func (transaction Transaction)SetString(blk files.Block, offset int, val string,oktolog  bool) {}