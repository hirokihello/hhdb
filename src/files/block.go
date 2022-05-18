package files;
import("strconv")

import (
	"crypto/sha256"
)

// これはページではないし、メモリと直接関係するような構造体でもない
// ただfileNameとblockNumber(論理的ブロック数)を持つだけ。
// どのblockかという識別用の構造体にすぎない
type Block struct {
	FileName string;
	BlockNumber int;
}

type BlockI interface {
	IsEqual() bool;
	ToString() string;
	HashCode() string;
}

func(a Block) IsEqual (b Block) bool {
	return a.FileName == b.FileName && a.BlockNumber == b.BlockNumber;
}

func (a Block) ToString () string {
	return "filename: " + a.FileName + ", block: " + strconv.Itoa(a.BlockNumber);
}

func(a Block) HashCode () string {
	buff := []byte(a.ToString());
	p := sha256.Sum256(buff);
	return string(p[:]);
}
