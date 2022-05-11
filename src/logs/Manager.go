package logs;

import "../dbFile"

func Append () dbFile.Block {
	block := dbFile.Block{FileName: "test", BlockNumber: 1};
	return block;
}