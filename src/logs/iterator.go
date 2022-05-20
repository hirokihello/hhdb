package logs;

import "github.com/hirokihello/hhdb/src/files"

type Iterator struct {
	FileManager files.Manager
	Block files.Block
	Page files.Page
	Boundary int
	CurrentPosition int
}

func (itr *Iterator)moveToBlock (block files.Block) {
	itr.FileManager.Read(block, itr.Page);
	itr.Boundary = itr.Page.GetInt(0);
	itr.CurrentPosition = itr.Boundary;
}

func (itr *Iterator) HasNext() bool {
	return itr.CurrentPosition < itr.FileManager.BlockSize || itr.Block.BlockNumber > 0;
}

func (itr *Iterator) Next() []byte {
	if(itr.CurrentPosition == itr.FileManager.BlockSize) {
		block
	}
}

func CreateIter (fileManager files.Manager, block files.Block) *Iterator {
	b := make([]byte, fileManager.BlockSize);
	page := files.LoadBufferToPage(b);
	itr := Iterator{FileManager: fileManager, Block: block, Page: page, Boundary: 0, CurrentPosition: 0};
	itr.moveToBlock(block)

	return &itr
}