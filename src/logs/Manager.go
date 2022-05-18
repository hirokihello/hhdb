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

func appendNewBlock (fileName string) files.Block {
	return files.Block{FileName: fileName, BlockNumber: 1}
}

func CreateLogManager (manager *files.Manager, fileName string) Manager {
	page := files.CreatePage(manager.BlockSize);
	// ファイルがいっぱいかどうかを返す。
	// なぜintなのか不明すぎる...
	// 普通にboolでええやん...
	length := manager.Length(fileName);
	var block files.Block;
	if(length == 0) {
		block = appendNewBlock(fileName)
	} else {
		block = files.Block{FileName: fileName, BlockNumber: length - 1}
		manager.Read(block, page);
	}
	return Manager{FileManager: *manager, LogFile: fileName, LogPage: page, LatestLsn: 0, LastSavedLsn: 0}
}