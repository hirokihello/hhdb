package db

import (
	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/logs"
)

const INTEGER_BYTES = 4;

type Db struct {
	FileManager   *files.Manager
	LogManager    *logs.Manager
	BufferManager *buffers.Manager
}

func CreateDB(path string, blockSize int, bufferPoolCount int) Db {
	fileManager := files.CreateManager(path, blockSize)
	logManager := logs.CreateManager(fileManager, path+"logfile.log")
	bufferManager := buffers.CreateManager(fileManager, *logManager, bufferPoolCount)

	return Db{
		FileManager:   fileManager,
		LogManager:    logManager,
		BufferManager: bufferManager,
	}
}
