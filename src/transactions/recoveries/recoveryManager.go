package recoveries

import (
	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/logs"
	transactionInterface "github.com/hirokihello/hhdb/src/transactions/interfaces"
	"golang.org/x/exp/slices"
)

type RecoveryManager struct {
	logManager    logs.Manager
	bufferManager buffers.Manager
	tx            any // 困ったら transaction interface を利用しよう。それまではこれでお茶を濁す
	txnum         int
}

// 何かログに書き込みたい時は、この struct に implement されているメソッドの中から選んで実行する

// この manager が呼び出されたトランザクションの commit を実行したいときに呼び出す
func (manager RecoveryManager) Commit() {
	// buffer pool 上での全ての変更をディスクに書き込む
	manager.bufferManager.FlushAll(manager.txnum)
	// そのトランザクションの commit log をログに書き込む
	lsn := CommitWriteRecordToLog(manager.logManager, manager.txnum)
	//　ログを保存して、transaction が確実に記録に残るようにする
	manager.logManager.Flush(lsn)
}

// この manager が呼び出されたトランザクションの rollback を実行したいときに呼び出す
func (manager RecoveryManager) Rollback() {
	// rollback　を行う
	manager.doRollback()
	// ディスクに変更を書き込む
	manager.bufferManager.FlushAll(manager.txnum)
	// そのトランザクションの rollback log をログに書き込む
	lsn := RollbackRecordWriteToLog(manager.logManager, manager.txnum)
	//　ログを保存して、transaction が確実に記録に残るようにする
	manager.logManager.Flush(lsn)
}

func (manager RecoveryManager) Recover() {
	manager.doRecover()
	// ディスクに変更を書き込む
	manager.bufferManager.FlushAll(manager.txnum)
	// そのトランザクションの checkpoint log をログに書き込む
	lsn := CheckpointRecordWriteToLog(manager.logManager)
	//　ログを保存して、transaction が確実に記録に残るようにする
	manager.logManager.Flush(lsn)
}

func (manager RecoveryManager) SetInt(buffer buffers.Buffer, offset int, newval int) int {
	oldval := buffer.Contents().GetInt(offset)
	block := buffer.Block()

	return SetIntRecordWriteToLog(manager.logManager, manager.txnum, block, offset, oldval)
}

func (manager RecoveryManager) SetString(buffer buffers.Buffer, offset int, newval string) int {
	oldval := buffer.Contents().GetString(offset)
	block := buffer.Block()

	return SetStringRecordWriteToLog(manager.logManager, manager.txnum, block, offset, oldval)
}

func (manager RecoveryManager) doRollback() {
	iterator := manager.logManager.Iterator()

	for iterator.HasNext() {
		bytes := iterator.Next()
		rec := CreateLogRecord(bytes)

		//　読み込んだレコードが、このリカバリーマネージャーを作っているトランザクションと一致している場合、undo して切り戻す
		if rec.TxNumber() == manager.txnum {
			// このトランザクションの start record まで遡ったら、それ以上戻す必要のある record はないはずなので処理を収量」
			if rec.Op() == START {
				return
			}

			rec.Undo()
		}
	}
}

// recovery を行うための関数
func (manager RecoveryManager) doRecover() {
	var finishedTxs []int
	iterator := manager.logManager.Iterator()

	for iterator.HasNext() {
		bytes := iterator.Next()
		record := CreateLogRecord(bytes)

		// quiscent なので、全ての transaction が終わったタイミングでこの checkpoint のログは書き込まれる。なのでこのログがある以前のものは見なくても良い
		if record.Op() == CHECKPOINT {
			return
		}

		// commit / rollback record だった場合、確実にディスクに書き込まれているので特に何もしない。既に終わったトランザクションとして記録しておく
		if record.Op() == COMMIT || record.Op() == ROLLBACK {
			finishedTxs = append(finishedTxs, record.TxNumber())

			// 終了したことが記録されていない変更ログなので、変更前の値に戻す
		} else if slices.Contains(finishedTxs, record.TxNumber()) {
			record.Undo()
		}
	}
}

func CreateRecoveryManager(
	logManager logs.Manager,
	bufferManager buffers.Manager,
	tx transactionInterface.TransactionInterface,
	txnum int,
) *RecoveryManager {
	return &RecoveryManager{
		logManager: logManager,
		bufferManager: bufferManager,
		tx: tx,
		txnum: txnum,
	}
}
