package engine;

import "github.com/hirokihello/hhdb/src/files"
import "github.com/hirokihello/hhdb/src/logs"
import "github.com/hirokihello/hhdb/src/buffers"

type Db struct {
	FileManager *files.Manager
	LogManager *logs.Manager
	BufferManager *buffers.Manager
}

func CreateDb (directoryPath string, blockSize int, bufN int) *Db {
	fileManager := files.CreateManager(directoryPath, blockSize);
	logManager := logs.CreateManager(fileManager, "log_file");
	bufferManager := buffers.CreateManager(bufN , logManager, fileManager);

	return &Db{FileManager: fileManager, LogManager: logManager, BufferManager: bufferManager}
}