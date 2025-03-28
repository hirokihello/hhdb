package transactions

import (
	"fmt"
	"sync"

	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
	"github.com/hirokihello/hhdb/src/transactions/concurrencies"
	transactionInterface "github.com/hirokihello/hhdb/src/transactions/interfaces"
	"github.com/hirokihello/hhdb/src/transactions/recoveries"
)

const END_OF_FILE int = -1

type Transaction struct {
	transactionInterface.TransactionI
	recoveryManager    *recoveries.RecoveryManager
	concurrencyManager *concurrencies.Manager
	bufferManager      *buffers.Manager
	fileManager        *files.Manager
	txNum              int
	myBuffers          *BufferList
}

func CreateTransaction(fileManager *files.Manager, logManager *logs.Manager, bufferManager *buffers.Manager) *Transaction {
	txNumber := nextTxNumber()
	transaction := &Transaction{
		fileManager:        fileManager,
		bufferManager:      bufferManager,
		txNum:              txNumber,
		concurrencyManager: concurrencies.CreateConcurrencyManager(),
		myBuffers:          CreateBufferList(bufferManager),
	}

	recoveryManager := recoveries.CreateRecoveryManager(*logManager, bufferManager, transaction, txNumber)
	transaction.recoveryManager = recoveryManager

	return transaction
}

// debug 用
func (transaction *Transaction) TxNum() int {
	return transaction.txNum
}

func (transaction *Transaction) Commit() {
	transaction.recoveryManager.Commit()
	// 保持していた buffer を全て解放する
	transaction.concurrencyManager.Release()
	transaction.myBuffers.unpinAll()

	fmt.Printf("transaction id %d was committed\n", transaction.txNum)
}

func (transaction *Transaction) Rollback() {
	transaction.recoveryManager.Rollback()
	transaction.concurrencyManager.Release()
	transaction.myBuffers.unpinAll()
	fmt.Printf("transaction id %d was rollbacked\n", transaction.txNum)
}

func (Transaction *Transaction) Recover() {
	Transaction.bufferManager.FlushAll(Transaction.txNum)
	Transaction.recoveryManager.Recover()
}

// この処理をして初めて、buffer にロードして使用できるようになる。必要なブロックは全て pin すること。
func (transaction *Transaction) Pin(blk files.Block) {
	transaction.myBuffers.pin(blk)
}

func (transaction *Transaction) UnPin(blk files.Block) {
	transaction.myBuffers.unPin(blk)
}

// block と offset を受け取ることで、そこからデータを取得
func (transaction *Transaction) GetInt(blk files.Block, offset int) int {
	transaction.concurrencyManager.XLock(blk)
	buffer := transaction.myBuffers.getBuffer(blk)
	return buffer.Contents().GetInt(offset)
}

// block と offset を受け取ることで、そこからデータを取得
func (transaction *Transaction) GetString(blk files.Block, offset int) string {
	transaction.concurrencyManager.XLock(blk)
	buffer := transaction.myBuffers.getBuffer(blk)

	return buffer.Contents().GetString(offset)
}

func (transaction *Transaction) SetInt(blk files.Block, offset int, val int, oktolog bool) {
	transaction.concurrencyManager.XLock(blk)

	buffer := transaction.myBuffers.getBuffer(blk)
	lsn := -1
	if oktolog {
		// fmt.Print("\n SetInt(blk files.Block, offset int, val int, oktolog bool) loggggeddddd \n")
		lsn = transaction.recoveryManager.SetInt(buffer, offset, val)
	}
	p := buffer.Contents()
	p.SetInt(offset, uint32(val))
	// oktolog が true であれば、その buffer の lsn を更新してログに記録が残るようにする
	buffer.SetModified(transaction.txNum, lsn)
}

func (transaction *Transaction) SetString(blk files.Block, offset int, val string, oktolog bool) {
	transaction.concurrencyManager.XLock(blk)
	buffer := transaction.myBuffers.getBuffer(blk)
	lsn := -1
	if oktolog {
		// ここでエラーが生じている
		lsn = transaction.recoveryManager.SetString(buffer, offset, val)
	}
	p := buffer.Contents()
	p.SetString(offset, val)
	// oktolog が true であれば、その buffer の lsn を更新してログに記録が残るようにする
	buffer.SetModified(transaction.txNum, lsn)
}

// ファイルに含まれるブロック数を返却する
func (transaction *Transaction) Size(fileName string) int {
	// dummy のブロックを作る。ファイル名の長さを知りたいだけなので、これで十分なんだけど。なんで作るんだろ？？
	dummyBlock := files.Block{FileName: fileName, Number: -1}
	transaction.concurrencyManager.XLock(dummyBlock)

	return transaction.fileManager.FileBlockLength(fileName)
}

func (transaction *Transaction) Append(fileName string) *files.Block {
	// dummy のブロックを作る。ファイル名の長さを知りたいだけなので、これで十分なんだけど。なんで作るんだろ？？
	dummyBlock := files.Block{FileName: fileName, Number: -1}
	transaction.concurrencyManager.XLock(dummyBlock)

	return transaction.fileManager.Append(fileName)
}

func (transaction *Transaction) BlockSize() int {
	return transaction.fileManager.BlockSize
}

func (transaction *Transaction) AvailableBuffers() int {
	return transaction.bufferManager.Available()
}

// global で一つの lock table を使用したいため。
var instance *NextTransactionNum
var once sync.Once

// lockTable を使用したい場合必ずここから呼び出す。さもなければ、関数ごとに mu が作成されてうまく共有されなくなってしまう....
func GetInstanceOfLockTable() *NextTransactionNum {
	once.Do(func() {
		instance = &NextTransactionNum{
			nextTransactionNum: 0,
		}
	})
	return instance
}

// プログラム全体で 一つの NextTransactionNum のみが存在するようにいい感じに引数として渡すなりする。ここは頑張る....
// 元の書籍では static 変数を使用することで、簡単に実装している。
type NextTransactionNum struct {
	mu                 sync.Mutex // guards
	nextTransactionNum int
}

func nextTxNumber() int {
	instance := GetInstanceOfLockTable()
	instance.mu.Lock()
	defer instance.mu.Unlock()
	instance.nextTransactionNum++
	return instance.nextTransactionNum
}
