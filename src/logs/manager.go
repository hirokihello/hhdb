package logs

import (
	"fmt"

	"github.com/hirokihello/hhdb/src/files"
)

// to do
// 排他処理の記述

// ログブロック/ページのそれぞれの最初の 4 byte はページの空き容量を保存するために使用されている。
// 実質使用可能なのは、manager.BlockSize - 4 のサイズ

type Manager struct {
	ManagerI
	fileManager  *files.Manager
	logFile      string     // log ファイルの名前
	logPage      files.Page // log file manager で使用するページオブジェクト
	currentBlock files.Block
	latestLSN    int // 現在ページに保持しているログレコードの数値
	lastSavedLSN int // 最後にディスクに書き込んだログレコードの数値
}

type ManagerI interface {
	Flush()  // page 上のログをファイルに書き込む
	Append() // 新しくファイル上にブロックを作成する
}

// public method /
// ページの内容を書き込む
func (manager *Manager) Flush(lsn int) {
	if manager.lastSavedLSN <= lsn {
		manager.flush()
	}
}

// iterator
func (manager *Manager) Iterator() *Iterator {
	manager.Flush(manager.lastSavedLSN)
	return createLogIterator(manager.fileManager, manager.currentBlock)
}

// log を logPage に保存する。
// 最新のログレコードの数値を返却する
// 右から左に向かってページの中を書き換えていくのに注意。右詰でログレコードを足していく
// byte 列のログレコードを引数として受け取り、ログとして保存する
func (manager *Manager) Append(logRecord []byte) int {
	boundary := manager.logPage.GetInt(0)
	recordSize := len(logRecord)
	// レコードは冒頭の 4 bytes を使用して、レコードサイズを保存するため
	bytesNeededForNewRecord := recordSize + 4

	// 先頭の 4 byte は残りの容量を示すために使用するため、それ以下の要領しかなければ新しくブロックを作る
	if boundary-bytesNeededForNewRecord < 4 {
		// 保存
		manager.flush()
		// 新しいブロックの作成
		manager.currentBlock = *manager.appendNewBlock()
		// boundary を新しくロードしたページの先頭の int に変更
		boundary = manager.logPage.GetInt(0)
	}

	recordPosition := boundary - bytesNeededForNewRecord
	// page にレコードの内容を書き込み
	manager.logPage.SetBytes(logRecord, recordPosition)
	// logPage の残りサイズを更新
	manager.logPage.SetInt(0, uint32(recordPosition))

	manager.latestLSN += 1

	return manager.latestLSN
}

// private
// page の内容をファイルに書き込む
func (manager *Manager) flush() {

	fmt.Printf("flushed block id: %d\n", manager.currentBlock.Number)
	// ファイルマネージャーを用いて、現在のログページの内容を、ファイルに書き込み
	manager.fileManager.Write(manager.currentBlock, manager.logPage)
	// 最後に保存したログを最新のものに更新
	manager.lastSavedLSN = manager.latestLSN
}

// 新しく block を作成する
func (manager *Manager) appendNewBlock() *files.Block {
	block := manager.fileManager.Append(manager.logFile)

	// 一番最初の 4 byte にブロックサイズを格納
	manager.logPage.SetInt(0, uint32(manager.fileManager.BlockSize))
	manager.fileManager.Write(*block, manager.logPage)

	return block
}

// 新しくログマネージャーを作成する
func CreateManager(fileManager *files.Manager, logFile string) *Manager {
	// ログ用に page を作成する、つまりログ用のメモリ領域を確保する
	logPage := files.CreatePage(fileManager.BlockSize)

	// ログファイルを読み込んでブロック数を取得。(ログファイルがない場合は勝手に作成される)
	logFileBlockSize := fileManager.FileBlockLength(logFile)

	var block files.Block
	// 初期状態の場合、初期化する。デフォルトでファイルが作られるときにブロックが作られるのでそのように挙動を修正する
	if logFileBlockSize == 1 {
		block = files.Block{FileName: logFile, Number: 0}
		logPage.SetInt(0, uint32(fileManager.BlockSize))
		fileManager.Write(block, logPage)
	} else if logFileBlockSize > 0 {
		// ログ用ファイルの最後のブロックを取得。取得できない場合=ログファイルが空の場合、,勝手にブロックが作られる
		block = files.Block{FileName: logFile, Number: logFileBlockSize - 1}
	} else {
		fmt.Errorf("error occured at log manager")
	}

	// 上で取得したブロックをメモリ上に読み込む。logPage 変数で扱えるようにする。
	fileManager.Read(block, logPage)

	return &Manager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      logPage,
		currentBlock: block,
		latestLSN:    0,
		lastSavedLSN: 0,
	}
}
