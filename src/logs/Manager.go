package logs;

import "github.com/hirokihello/hhdb/src/file"

func Append () file.Block {
	block := dbFile.Block{FileName: "test", BlockNumber: 1};
	return block;
}