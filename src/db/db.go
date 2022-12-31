package db;

import "github.com/hirokihello/hhdb/src/files"
import "github.com/hirokihello/hhdb/src/logs"

type Db struct {
	FileManager *files.Manager
	LogManager *logs.Manager
}

func CreateDB (path string , size int) Db {
	fileManager := files.CreateManager(path, size)
	logManager := logs.CreateManager(fileManager, path + "logfile.log")

	return Db{
		FileManager: fileManager,
		LogManager: logManager,
	};
}