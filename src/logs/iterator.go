package logs;

import "fmt"

import "github.com/hirokihello/hhdb/src/files"

type Iterator struct {
	FileManager files.Manager
	Block files.Block
	Page files.Page
	Boundary int
	CurrentPosition int
}

func (itr *Iterator) moveToBlock (block *files.Block) {
	itr.FileManager.Read(*block, itr.Page);
	itr.Boundary = itr.Page.GetInt(0);
	itr.CurrentPosition = itr.Boundary;

	itr.Block = *block;
}

func (itr *Iterator) HasNext() bool {
	return itr.CurrentPosition < itr.FileManager.BlockSize || itr.Block.BlockNumber > 0;
}

func (itr *Iterator) Next() []byte {
	if(itr.CurrentPosition == itr.FileManager.BlockSize) {
		new_block := files.Block{BlockNumber: itr.Block.BlockNumber - 1, FileName: itr.Block.FileName};
		itr.moveToBlock(&new_block);
	}

	records := itr.Page.GetBytes(itr.CurrentPosition);
	itr.CurrentPosition += 4 + len(records);

	return records;
}

func CreateIter (fileManager files.Manager, block files.Block) *Iterator {
	b := make([]byte, fileManager.BlockSize);
	page := files.LoadBufferToPage(b);
	itr := Iterator{FileManager: fileManager, Block: block, Page: page, Boundary: 0, CurrentPosition: 0};
	itr.moveToBlock(&block)

	fmt.Println(itr.Block.BlockNumber);
	return &itr
}