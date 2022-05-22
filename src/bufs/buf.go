package bufs;

import "github.com/hirokihello/hhdb/src/files"
import "github.com/hirokihello/hhdb/src/logs"
type Buf struct {
	FileManger *files.Manager;
	LogManger *logs.Manager;
	Contents *files.Page;
	Block files.Block;
	Pins int;
	TxNum int;
	Lsn int;
}
// ログ・シーケンス番号 (LSN)
// ログの番号
// txnumはtransaction number
func (buf *Buf) SetModified (txNum int, lsn int) {
	buf.TxNum = txNum;
	if(lsn >= 0) {
		buf.Lsn = lsn;
	}
}

func (buf *Buf) IsPinned () bool {
	return buf.Pins > 0;
}

func (buf *Buf) ModifyingTxNum () int {
	return buf.TxNum;
}

func (buf *Buf) Flush () {
	if(buf.TxNum >= 0) {
		buf.FileManger.Write(buf.Block, *buf.Contents);
		buf.LogManger.Flush(buf.Lsn);
		buf.TxNum = -1;
	}
}

// 新規でbufに格納したいblockがくると、既存のものをflushして新しいblockに置き換える
func (buf *Buf) AssignToBlock (block files.Block) {
	buf.Flush();
	buf.Block = block;
	buf.FileManger.Read(block, *buf.Contents);
	buf.Pins++;
}

func (buf *Buf) UnPin () {
	buf.Pins--;
}

func (buf *Buf) Pin () {
	buf.Pins++;
}

func CreateBuf (logManger *logs.Manager, fileManager *files.Manager) *Buf {
	p := files.CreatePage(fileManager.BlockSize);
	newBuf := Buf{FileManger: fileManager, LogManger: logManger, Contents: &p, Pins: 0, TxNum: -1, Lsn: -1};
	return &newBuf;
}