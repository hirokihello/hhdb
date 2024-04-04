package files;

import (
	"crypto/sha256"
	"strconv"
)

// これはページではないし、メモリと直接関係するような構造体でもない
// ただfileNameとblockNumber(論理的ブロック数)を持つだけ。
// どのblockかという識別用の構造体にすぎない
type Block struct {
	FileName string;
	Number int;
}

// Block のインターフェース
type BlockI interface {
	IsEqual() bool; // block が等しいか検証する
	ToString() string; // block のファイル名とブロック番号を合わせたものを返す
	HashCode() string; // ブロックに固有のハッシュ値を返却する
}

func(a Block) IsEqual (b Block) bool {
	return a.FileName == b.FileName && a.Number == b.Number;
}

func (a Block) ToString () string {
	return "filename: " + a.FileName + ", block: " + strconv.Itoa(a.Number);
}

func(a Block) HashCode () string {
	buff := []byte(a.ToString());
	p := sha256.Sum256(buff);
	return string(p[:]);
}
