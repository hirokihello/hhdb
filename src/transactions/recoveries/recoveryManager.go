package recoveries

import (
	"fmt"

	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/logs"
	transactionInterface "github.com/hirokihello/hhdb/src/transactions/interfaces"
	"golang.org/x/exp/slices"
)

type RecoveryManager struct {
	logManager    logs.Manager
	bufferManager *buffers.Manager
	tx            transactionInterface.TransactionI // 困ったら transaction interface を利用しよう。それまではこれでお茶を濁す
	txNum         int
}

// 何かログに書き込みたい時は、この struct に implement されているメソッドの中から選んで実行する

// この manager が呼び出されたトランザクションの commit を実行したいときに呼び出す
func (manager RecoveryManager) Commit() {
	// buffer pool 上での全ての変更をディスクに書き込む
	manager.bufferManager.FlushAll(manager.txNum)

	// そのトランザクションの commit log をログに書き込むため、ログをページ上に作成する
	lsn := CommitWriteRecordToLog(manager.logManager, manager.txNum)
	//　ログを保存して、transaction が確実に記録に残るようにする
	manager.logManager.Flush(lsn)
}

// この manager が呼び出されたトランザクションの rollback を実行したいときに呼び出す
func (manager RecoveryManager) Rollback() {
	fmt.Print("\nrecovery manager: rollback will begin!!!!!\n")
	// buffer pool 上での全ての変更をディスクに書き込む
	// rollback　を行う
	manager.doRollback()
	// ディスクに変更を書き込む
	manager.bufferManager.FlushAll(manager.txNum)
	// そのトランザクションの rollback log をログに書き込む
	lsn := RollbackRecordWriteToLog(manager.logManager, manager.txNum)
	//　ログを保存して、transaction が確実に記録に残るようにする
	manager.logManager.Flush(lsn)
}

func (manager RecoveryManager) Recover() {
	manager.doRecover()
	// ディスクに変更を書き込む
	manager.bufferManager.FlushAll(manager.txNum)
	// そのトランザクションの checkpoint log をログに書き込む
	lsn := CheckpointRecordWriteToLog(manager.logManager)
	//　ログを保存して、transaction が確実に記録に残るようにする
	manager.logManager.Flush(lsn)
}

// func (manager RecoveryManager) FlushLogs() {
// 	// そのトランザクションの checkpoint log をログに書き込む
// 	lsn := CheckpointRecordWriteToLog(manager.logManager)
// 	//　ログを保存して、transaction が確実に記録に残るようにする
// 	manager.logManager.Flush(lsn)
// }

func (manager RecoveryManager) SetInt(buffer *buffers.Buffer, offset int, newval int) int {
	oldval := buffer.Contents().GetInt(offset)
	block := buffer.Block()

	return SetIntRecordWriteToLog(manager.logManager, manager.txNum, *block, offset, oldval)
}

func (manager RecoveryManager) SetString(buffer *buffers.Buffer, offset int, newval string) int {
	oldval := buffer.Contents().GetString(offset)
	block := buffer.Block()

	return SetStringRecordWriteToLog(manager.logManager, manager.txNum, *block, offset, oldval)
}

func (manager RecoveryManager) doRollback() {
	iterator := manager.logManager.Iterator()

	for iterator.HasNext() {
		bytes := iterator.Next()
		rec := CreateLogRecord(bytes)

		if rec != nil {
			fmt.Print("iterator has next\n")
			fmt.Printf("rec: %T \n", rec)
			fmt.Printf("Op: %d \n", rec.Op())
			fmt.Printf("TxNumber: %d \n", rec.TxNumber())
			fmt.Printf("manager.TxNum: %d \n", manager.txNum)
			fmt.Print("iterator \n")
			//　読み込んだレコードが、このリカバリーマネージャーを作っているトランザクションと一致している場合、undo して切り戻す
			if rec.TxNumber() == manager.txNum {
				// このトランザクションの start record まで遡ったら、それ以上戻す必要のある record はないはずなので処理を収量」

				if rec.Op() == START {
					return
				}

				// string / int の update record 以外は
				switch record := rec.(type) {
				case SetStringLogRecord:
					record.UnDo(manager.tx)
				case SetIntLogRecord:
					record.UnDo(manager.tx)
				default:
				}
			}
		}
	}
}

// recovery を行うための関数
func (manager RecoveryManager) doRecover() {
	var finishedTxs []int
	// iterator で、戻す必要のあるログがきちんと取得できていない。
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
			switch rec := record.(type) {
			case SetStringLogRecord:
				rec.UnDo(manager.tx)
			case SetIntLogRecord:
				rec.UnDo(manager.tx)
			default:
			}
		}
	}
}

func CreateRecoveryManager(
	logManager logs.Manager,
	bufferManager *buffers.Manager,
	tx transactionInterface.TransactionI,
	txNum int,
) *RecoveryManager {
	StartRecordWriteToLog(logManager, txNum)

	return &RecoveryManager{
		logManager:    logManager,
		bufferManager: bufferManager,
		tx:            tx,
		txNum:         txNum,
	}
}
