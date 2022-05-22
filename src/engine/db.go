package engine;

import "github.com/hirokihello/hhdb/src/files"
import "github.com/hirokihello/hhdb/src/logs"
import "github.com/hirokihello/hhdb/src/bufs"

type Db struct {
	FileManager *files.Manager
	LogManager *logs.Manager
	BufferManager *bufs.Manager
}

func CreateDb (directoryPath string, blockSize int, bufN int) *Db {
	fileManager := files.CreateManager(directoryPath, blockSize);
	logManager := logs.CreateManager(fileManager, "log_file");
	bufferManager := bufs.CreateManager(bufN , logManager, fileManager);

	return &Db{FileManager: fileManager, LogManager: logManager, BufferManager: bufferManager}
}