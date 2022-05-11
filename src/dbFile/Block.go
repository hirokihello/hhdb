package dbFile;
import("strconv")

import (
	"crypto/sha256"
)

type Block struct {
	FileName string;
	BlockNumber int;
}

type BlockI interface {
	IsEqual();
	ToString();
	HashCode();
}

func(a Block) IsEqual (b Block) bool {
	return a.FileName == b.FileName && a.BlockNumber == b.BlockNumber;
}

func (a Block) ToString () string {
	return "filename: " + a.FileName + ", block: " + strconv.Itoa(a.BlockNumber);
}

func(a Block) HashCode ()string {
	buff := []byte(a.ToString());
	p := sha256.Sum256(buff);
	return string(p[:]);
}
