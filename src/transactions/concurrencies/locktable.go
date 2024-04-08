package concurrencies

import (
	"errors"
	"sync"
	"time"

	"github.com/hirokihello/hhdb/src/files"
)

const MAX_LOCK_TIME = 10 // 10 sec たったら dead lock が発生しているとみなして、ロックを解除する

// global で一つの lock table を使用したいため。
var instance *LockTable
var once sync.Once

// lockTable を使用したい場合必ずここから呼び出す。さもなければ、関数ごとに mu が作成されてうまく共有されなくなってしまう....
func GetInstanceOfLockTable() *LockTable {
	once.Do(func() {
		instance = &LockTable{
			table: make(map[files.Block]int),
		}
	})
	return instance
}

// プログラム全体で 一つの LockTable のみが存在するようにいい感じに引数として渡すなりする。ここは頑張る....
// 元の書籍では static 変数を使用することで、簡単に実装している。
type LockTable struct {
	mu    sync.Mutex // guards
	table map[files.Block]int
}

// sLock を行う。xLock が行われていないことを確認する
func (lockTable *LockTable) sLock(blk files.Block) (any, error) {
	// lock 処理を行う
	lockTable.mu.Lock()
	defer lockTable.mu.Unlock()

	now := time.Now().Unix()

	// xLock が既に他の manager により取得されている場合、基本はまつ。待ち時間が長過ぎれば抜ける
	for lockTable.hasXlock(blk) && !waitingTooLong(int(now)) {
		time.Sleep(10 * time.Millisecond)
	}
	// 待っても xLock が解放されない場合、エラーを返す
	if lockTable.hasXlock(blk) {
		return nil, errors.New("LockAbortException")
	}

	// xLock 取得
	val := lockTable.getLockValue(blk)
	lockTable.table[blk] = val + 1

	// 本来は interrputed exception の処理がある。InterruptedException。
	// スレッドに割り込みが発生した時にでる例外らしい。
	// しかし、golang でやるのめんどくさそうなのでやらない。問題が起きたらこのコメント見て頑張ってくれ。

	return nil, nil
}

func (lockTable *LockTable) xLock(blk files.Block) (any, error) {
	// lock 処理を行う
	lockTable.mu.Lock()
	defer lockTable.mu.Unlock()

	now := time.Now().Unix()

	// sLock が既に他の manager により取得されている場合、基本はまつ。待ち時間が長過ぎれば抜ける
	for lockTable.hasOtherSLock(blk) && !waitingTooLong(int(now)) {
		time.Sleep(10 * time.Millisecond) // 0.1 sec sleep
	}
	// 待っても sLock が解放されない場合、エラーを返す
	if lockTable.hasOtherSLock(blk) {
		return nil, errors.New("LockAbortException")
	}

	// xLock を取得でき次第、値を -1 に変更する
	lockTable.table[blk] = -1

	// 本来は interrputed exception の処理がある。InterruptedException。
	// スレッドに割り込みが発生した時にでる例外らしい。
	// しかし、golang でやるのめんどくさそうなのでやらない。問題が起きたらこのコメント見て頑張ってくれ。

	return nil, nil
}

// lock を解除する
func (lockTable *LockTable) UnLock(blk files.Block) {
	val := lockTable.getLockValue(blk)
	// 現在 slock を取っている場合 val は 1 より大きい = sLock が取得されている場合、一つ解除する
	if val > 1 {
		lockTable.table[blk] = val - 1
		// xLock を取得していた場合は、そもそも key を削除しておく。xlock を取得できる場合、他の transaction が sLock を取っていることはない。
	} else {
		delete(lockTable.table, blk)
	}
}

// 引数の時刻から MAX_LOCK_TIME sec 以上経っているか
func waitingTooLong(start int) bool {
	return int(time.Now().Unix())-start > MAX_LOCK_TIME
}

// xlock が取得されているか確認
func (lockTable *LockTable) hasXlock(blk files.Block) bool {
	return lockTable.getLockValue(blk) < 0
}

// 他に slock が取得されているか確認
func (lockTable *LockTable) hasOtherSLock(blk files.Block) bool {
	return lockTable.getLockValue(blk) > 1
}

func (lockTable *LockTable) getLockValue(blk files.Block) int {
	return lockTable.table[blk]
}
