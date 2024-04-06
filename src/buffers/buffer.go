package buffers

import (
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

type Buffer struct {
	fileManager *files.Manager // buffer manager が作り出した buffer pool 共通の file manager
	logManager  logs.Manager // buffer manager が作り出した buffer pool 共通のログ管理オブジェクト。これを使って transaction の結果のログを書き出していく
	block       *files.Block
	contents    *files.Page // メモリにロードした中身を保持して返却する
	pins        int        // 幾つのクライアントから pin されているかを管理
	txNum       int        // transaction 番号
	lsn         int        // 変更があった場合、どのログに書き込まれているのかを保持する
}

func CreateBuffer(fileManager *files.Manager, logManager logs.Manager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		block:       nil,
		contents:    files.CreatePage(fileManager.BlockSize),
		pins:        0,
		txNum:       -1,
		lsn:         -1,
	}
}

// 変更された場合に、ログのレコードと transaction の情報を更新する
func (buffer *Buffer) SetModified(txNum int, lsn int) {
	buffer.txNum = txNum
	if lsn >= 0 {
		buffer.lsn = lsn
	}
}

// pin されているかを判定する
func (buffer *Buffer) IsPinned() bool {
	return buffer.pins > 0
}

// このバッファーの最新の transaction id を取得する
func (buffer *Buffer) ModifyingTx() int {
	return buffer.txNum
}

// block を buffer に読み込む
func (buffer *Buffer) AssignToBlock(block *files.Block) {
	buffer.flush()
	//引数のブロックを現在のバッファーに読み込む
	buffer.block = block

	// 新しく読み込んだブロックの内容をメモリ上のページに読み込む
	buffer.fileManager.Read(*buffer.block, buffer.Contents())

	// pin の初期化
	buffer.pins = 0
}

// ログに書き込み、ディスクに書き込む
func (buffer *Buffer) flush() {
	if buffer.txNum >= 0 {
		buffer.logManager.Flush(buffer.lsn)
		buffer.fileManager.Write(*buffer.block, buffer.Contents())
		buffer.txNum = -1
	}
}

func (buffer *Buffer) Pin() {
	buffer.pins++
}

func (buffer *Buffer) Unpin() {
	buffer.pins--
}

func (buffer *Buffer) Contents() files.Page {
	return buffer.contents
}


func (buffer *Buffer) Block() files.Block {
	return buffer.block
}