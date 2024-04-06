package buffers

import (
	"errors"
	"sync"
	"time"

	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

// 10 秒
var MAX_WAIT_TIME int = 5

type Manager struct {
	bufferPool         []*Buffer // buffer = memory 上に保持している
	numAvailableBuffer int
	mu                 sync.Mutex     // 排他処理を行うためのもの
	wg                 sync.WaitGroup // pin の処理を制御するため
}

func (manager *Manager) Available() int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.numAvailableBuffer
}

// 引数の transaction id と一致する transaction 番号をもつ buffer の内容をディスクに書き込む
func (manager *Manager) FlushAll(txnum int) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	for _, buffer := range manager.bufferPool {
		// 現在修正したトランザクション id と一致する場合に flush で書き込む
		if buffer.ModifyingTx() == txnum {
			buffer.flush()
		}
	}
}

func (manager *Manager) Unpin(buffer *Buffer) {
	buffer.Unpin()
	// pin されていない場合
	if !buffer.IsPinned() {
		manager.numAvailableBuffer++
	}
}

func (manager *Manager) Pin(block *files.Block) (*Buffer, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	time := time.Now().Unix()
	buffer := manager.tryToPin(block)

	for buffer == nil && !waitingTooLong(time) {
		// wait
		buffer = manager.tryToPin(block)
	}

	//　待っても buffer が取得できない場合
	if buffer == nil {
		return nil, errors.New("too long wait time")
	}

	return buffer, nil
}

// buffer pool で管理しているバッファーに読み込む
func (manager *Manager) tryToPin(block *files.Block) *Buffer {
	// すでに buffer にブロックが読み込まれているかチェック
	buffer := manager.findExistingBuffer(block)

	// 読み込まれていない場合、どこかのバッファーに読み込む
	if buffer == nil {
		// 空いているバッファーがあれば、そこにブロックを読みこむ
		buffer = manager.chooseUnpinnedBuffer()
		// 読み込めなかった場合は nil を返す
		if buffer == nil {
			return nil
		}

		// buffer に該当するブロックの内容を読み込む
		buffer.AssignToBlock(block)
	}

	// buffer がまだ pin されていない場合、使用可能なバッファーの数を減らす
	if !buffer.IsPinned() {
		manager.numAvailableBuffer--
	}

	// buffer に関して、pin する
	buffer.Pin()

	// 読み込めた buffer を返却する
	return buffer
}

// すでにどこかのバッファーにそのブロックがロードされていれば、そのバッファーを返す
func (manager *Manager) findExistingBuffer(block *files.Block) *Buffer {
	for _, buffer := range manager.bufferPool {
		blockOfBuffer := buffer.block
		if blockOfBuffer != nil && blockOfBuffer.IsEqual(*block) {
			return buffer
		}
	}

	return nil
}

// 空いている buffer が存在すれば、それを返す。存在しなければ nil を返す
func (manager *Manager) chooseUnpinnedBuffer() *Buffer {
	// ナイーブな実装。先頭から空いているか見ていく
	for _, buffer := range manager.bufferPool {
		if !buffer.IsPinned() {
			return buffer
		}
	}

	return nil
}

// 最初にトライしてから 10 秒以上経過した場合、待ちすぎなので true が返却される
func waitingTooLong(startAt int64) bool {
	return time.Now().Unix()-startAt > int64(MAX_WAIT_TIME)
}

func CreateManager(fileManager *files.Manager, logManager logs.Manager, bufferCount int) *Manager {
	bufferPool := make([]*Buffer, bufferCount)

	for i := 0; i < bufferCount; i++ {
		bufferPool[i] = CreateBuffer(fileManager, logManager)
	}

	return &Manager{
		bufferPool:         bufferPool,
		numAvailableBuffer: bufferCount,
	}
}