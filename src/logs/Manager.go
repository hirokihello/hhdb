package logs;

import "github.com/hirokihello/hhdb/src/files"

type Manager struct {
	FileManager files.Manager
	LogFile string
	LogPage files.Page
	CurrentBlock files.Block
	LatestLsn int
	LastSavedLsn int
}

func (lm *Manager) flush() {
	lm.FileManager.Write(lm.CurrentBlock, lm.LogPage);
	lm.LastSavedLsn = lm.LatestLsn;
}

func (lm *Manager) Flush(lsn int) {
	if lsn > lm.LastSavedLsn {
		lm.flush();
	}
}

func (lm *Manager) Iterator() {
	lm.flush();
}

func (lm *Manager) appendNewBlock() *files.Block {
	block := lm.FileManager.Append(lm.LogFile);
	lm.LogPage.SetInt(uint32(lm.FileManager.BlockSize), 0);
	lm.FileManager.Write(block, lm.LogPage);
	return &block;
}

func (lm *Manager) Append(records []byte) {
	boundary := lm.LogPage.GetInt(0);
	records_size := len(records);
	bytes_needed := records_size + 4;

	// 末尾に4byteのintをつけるため
	if(boundary < bytes_needed + 4) {
		lm.flush();
		lm.CurrentBlock = *lm.appendNewBlock();
		boundary = lm.LogPage.GetInt(0);
	}

	recordPosition := boundary - bytes_needed;
	lm.LogPage.SetBytes(records, recordPosition);
	lm.LogPage.SetInt(uint32(recordPosition), 0);
	lm.LatestLsn++;
}

// 初期化で使うメソッド。色々と管理しているので重要
func CreateLogManager (manager *files.Manager, fileName string) Manager {
	page := files.CreatePage(manager.BlockSize);
	// ファイルがいっぱいかどうかを返す。
	// なぜintなのか不明すぎる...
	// 普通にboolでええやん...
	length := manager.Length(fileName);
	var block files.Block;
	logManager := Manager{FileManager: *manager, LogFile: fileName, LogPage: page, LatestLsn: 0, LastSavedLsn: 0};
	if(length == 0) {
		logManager.CurrentBlock = *logManager.appendNewBlock();
	} else {
		block = files.Block{FileName: fileName, BlockNumber: length - 1}
		manager.Read(block, logManager.LogPage);
		logManager.CurrentBlock = block;
	}
	return logManager;
}