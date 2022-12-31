package logs

import (
	"fmt"

	"github.com/hirokihello/hhdb/src/files"
)

// to do
// 排他処理の記述

// ログページの最初の 4 byte はページの空き容量を保存するために使用されている。
// 実質使用可能なのは、manager.BlockSize - 4 のサイズ

type Manager struct {
	fileManager  *files.Manager
	logFile      string     // log ファイルの名前
	logPage      files.Page // log file manager で使用するページオブジェクト
	currentBlock files.Block
	latestLSN    int // 現在ページに保持しているログレコードの数値
	lastSavedLSN int // 最後にディスクに書き込んだログレコードの数値
}

type ManagerInterface interface {
	Flush()
	Append()
}

// 新しくログマネージャーを作成する
func CreateManager(fileManager *files.Manager, logFile string) *Manager {
	// ログ用の Page を作成する
	logPage := files.CreatePage(fileManager.BlockSize)
	logFileBlockSize := fileManager.FileBlockLength(logFile)
	var block files.Block
	// まだ書き込まれていない場合
	if logFileBlockSize == 0 {
		block = *fileManager.Append(logFile)
		logPage.SetInt(0, uint32(fileManager.BlockSize))
		fileManager.Write(block, logPage)
		// すでに書き込まれている場合
	} else {
		// そのファイルの最後のブロックを取得
		block = files.Block{FileName: logFile, Number: logFileBlockSize - 1}
		// 最後のブロックをページに読み込んでおく
		fileManager.Read(block, logPage)
	}

	return &Manager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      logPage,
		currentBlock: block,
		latestLSN:    0,
		lastSavedLSN: 0,
	}
}

// ページの内容を書き込む外部向け interface
func (manager *Manager) Flush(lsn int) {
	if manager.lastSavedLSN < lsn {
		manager.flush()
	}
}

// page の内容を log ファイルに書き込む
// 最新のログレコードの数値を返却する
func (manager *Manager) Append(logRecord []byte) int {
	boundary := manager.logPage.GetInt(0)
	recordSize := len(logRecord)

	// 最初に 4 bytes を使用して、レコードサイズを保存するため
	bytesNeededForNewRecord := recordSize + 4

	// 先頭の 4 byte は残りの容量を示すために使用するため
	if boundary-bytesNeededForNewRecord < 5 {
		// 保存
		manager.flush()

		// 新しいブロックの作成と、ディスクへの書き込み
		block := *manager.fileManager.Append(manager.logFile)
		manager.logPage.SetInt(0, uint32(manager.fileManager.BlockSize))
		manager.fileManager.Write(block, manager.logPage)

		// boundary を新しくロードしたページの先頭の int に変更
		boundary = manager.logPage.GetInt(0)
	}

	recordPosition := boundary - bytesNeededForNewRecord
	// page にレコードの内容を書き込み
	manager.logPage.SetBytes(logRecord, recordPosition)
	// logPage の容量を更新
	manager.logPage.SetInt(0, uint32(recordPosition))

	manager.latestLSN += 1

	return manager.latestLSN
}

func (manager *Manager) Iterator() *Iterator {
	// manager.flush()

	return createLogIterator(manager.fileManager, manager.currentBlock)
}

// page の内容をファイルに書き込む
func (manager *Manager) flush() {
	fmt.Println("saved!!!!!!")
	// ファイルマネージャーを用いて、現在のログページの内容を、ファイルに書き込み
	manager.fileManager.Write(manager.currentBlock, manager.logPage)
	// 最後に保存したログを最新のものに更新
	manager.lastSavedLSN = manager.latestLSN
}
