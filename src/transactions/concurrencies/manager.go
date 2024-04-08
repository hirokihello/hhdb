package concurrencies

import (
	"github.com/hirokihello/hhdb/src/files"
)

type Manager struct {
	lockTable *LockTable
	locks     map[files.Block]string
}

const SLOCK = "S"
const XLOCK = "X"

func (manager *Manager) SLock(blk files.Block) {
	// まだこの concurrency manager でロックを取得していない場合、ロックを追加する
	if manager.locks[blk] == "" {
		manager.lockTable.sLock(blk)
		manager.locks[blk] = SLOCK
	}
}

func (manager *Manager) XLock(blk files.Block) {
	// まだ x lock を取得していない場合に処理を行う
	if !manager.hasXLock(blk) {
		// まずは sLock を取得する
		manager.SLock(blk)
		// xLock を取得する
		manager.lockTable.xLock(blk)
		// lock テーブルに書き込む
		manager.locks[blk] = XLOCK
	}
}

// この manager で保持している全てのロックを解除する
func (manager *Manager) Release(blk files.Block) {
	for key := range manager.locks {
		manager.lockTable.UnLock(key)
		delete(manager.locks, key)
	}
}

func (manager *Manager) hasXLock(blk files.Block) bool {
	return manager.locks[blk] == XLOCK
}

// concurrency manager を使用したい場合、下記から初期化処理を行う
func CreateConcurrencyManager() *Manager {
	return &Manager{
		lockTable: GetInstanceOfLockTable(),
	}
}
