package logs

import (
	"fmt"

	"github.com/hirokihello/hhdb/src/files"
)

type Iterator struct {
	fileManager     *files.Manager
	block           files.Block
	Page            files.Page
	Boundary        int
	currentPosition int
}

func (itr *Iterator) HasNext() bool {
	return itr.currentPosition < itr.fileManager.BlockSize || itr.block.Number > 0
}

func (itr *Iterator) Next() []byte {
	if itr.currentPosition == itr.fileManager.BlockSize {
		itr.block = files.Block{Number: itr.block.Number - 1, FileName: itr.block.FileName}
		itr.moveToBlock(&itr.block)
	}
	fmt.Println(itr.block.Number);
	fmt.Println(itr.currentPosition);

	records := itr.Page.GetBytes(itr.currentPosition)
	itr.currentPosition += 4 + len(records)
	return records
}

func createLogIterator(fileManager *files.Manager, block files.Block) *Iterator {
	b := make([]byte, fileManager.BlockSize)
	page := files.LoadBufferToPage(b)
	itr := Iterator{fileManager: fileManager, block: block, Page: page, Boundary: 0, currentPosition: 0}
	itr.moveToBlock(&block)

	return &itr
}

func (itr *Iterator) moveToBlock(block *files.Block) {
	itr.fileManager.Read(*block, itr.Page)
	itr.Boundary = itr.Page.GetInt(0)
	itr.currentPosition = itr.Boundary
}
