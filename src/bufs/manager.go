package bufs;

import "github.com/hirokihello/hhdb/src/files"
import "github.com/hirokihello/hhdb/src/logs"
import  (
	"time"
	"sync"
)
const MAX_TIME int = 10;

type Manager struct {
	BufPool []*Buf;
	NumAvailableBuf int;
	mu sync.Mutex // guards
}

func (m *Manager) Available() int { return m.NumAvailableBuf }

func (m *Manager) Unpin (buf *Buf) {
	buf.UnPin();
	if(!buf.IsPinned()) {
		m.NumAvailableBuf++;
		// clientに対してnotifyする必要がある
		// NotifyAll();
	}
}

// bufを返す。
// できなければ例外を起こす
func (m *Manager) Pin (block *files.Block) *Buf {
	m.mu.Lock();
	timeStamp := time.Now().Unix();
	buf, err := m.tryToPin(block);

	// 既存のものが取得できていないかつ時間がかかっていなければ
	for err != false && !m.waitingTimeTooLong(int(timeStamp)) {
		buf, err = m.tryToPin(block);
	}

	m.mu.Unlock();
	// 問題が特に起きてなければbufを返す
	if(err != true) {
		return buf;
	} else {
		panic("deadlock occurred")
	}
}

// 成功したらそのBufを返す。
// 失敗したら初期化状態のbufferとerr = trueを返す。
func (m *Manager) tryToPin (block *files.Block) (*Buf, bool) {
	buf, err := m.findExistingBuffer(block);

	if(err != false) {
		buf, err = m.chooseUnPinnedBuffer();
		if(err != false) {
			// すべてのbufがpinされていて新規に割り当てられない状態
			defaultBuf := Buf{};
			return &defaultBuf, true;
		}
		buf.AssignToBlock(*block);
	}

	// もしまだ割り当てたbufがpinされていなければこれからpinするので...
	if !buf.IsPinned() {
		m.NumAvailableBuf--;
	}

	// 新規にpinされている数をplusする
	buf.Pin();

	return buf, err;
}

func (m *Manager) FlushAll (txNum int) {
	m.mu.Lock();

	for _, buf := range m.BufPool {
		if (buf.ModifyingTxNum() == txNum) {
			buf.Flush();
		}
	}

	m.mu.Unlock();
}

func (m *Manager) waitingTimeTooLong (startUnixTime int) bool {
	return int(time.Now().Unix()) - startUnixTime > MAX_TIME;
}


func (m *Manager) findExistingBuffer (block *files.Block) (*Buf, bool) {
	for _, b := range m.BufPool {
		blk := b.Block;

		if(block.IsEqual(blk)) {
			return b, false;
		}
	}

	defaultBuf := Buf{};
	return &defaultBuf, true;
}
func (m *Manager) chooseUnPinnedBuffer() (*Buf, bool) {
	// pinされていないbufがその中に存在すればそれを返す。
	for _, b := range m.BufPool {
		if(!b.IsPinned()) {
			return b, false;
		}
	}

	defaultBuf := Buf{};
	return &defaultBuf, true;
}


func CreateManager (bufN int, logManager *logs.Manager, filesManager *files.Manager) *Manager {
	bufPool := make([]*Buf, bufN);
	for i := 0; i < bufN; i++ {
		bufPool[i] = CreateBuf(logManager, filesManager);
	}

	return &Manager{BufPool: bufPool, NumAvailableBuf: bufN};
}
