package files

import (
	"encoding/binary"
)

// ファイルの内容=ブロックをメモリ上で扱うためのオブジェクト。ファイルを memory に読み込ませたオブジェクト
type PageI interface {
	GetString(offset int) string      // page から文字列を読み込む
	GetInt(offset int) int            // page から数値を読み込む
	SetString(offset int, str string) // page に文字列を保存する
	SetInt(offset int, num uint32)    // page に数値を保存する
	Contents() []byte                 // page の内容を返す
}

// ファイルの内容=ブロックをメモリ上で扱うためのオブジェクト。ファイルを memory に読み込ませたオブジェクト
type Page struct {
	PageI
	ByteBuffer []byte
}

// ページ内の指定された箇所の位置にある blob を取得する
func (a *Page) GetBytes(offset int) []byte {
	// 最初の要素に長さが入っている。これは仕様。
	// 実際の要素の長さを取得
	size := a.GetInt(offset)
	// 最初の要素は 4 byte 分なので、その次が最初の要素となる
	start := offset + 4
	return a.ByteBuffer[start : start+size]
}

// ページ内の指定された箇所を読み込み、格納されている文字列を返す
func (a Page) GetString(offset int) string {
	buf := a.GetBytes(offset)
	return string(buf)
}

// 指定された位置を読み込み、その数値を返す
func (a Page) GetInt(offset int) int {
	// ページ内の指定された箇所の位置にある 4 byte を読み込んで数値に変換する。
	// 4 byte なのは仕様。深く考えない方が良い。
	return int(binary.LittleEndian.Uint32(a.ByteBuffer[offset : offset+4]))
}

// page に引数で渡された num を書き込む
// 引数の uint32 はこの DB の制約によるもの。
func (a Page) SetInt(offset int, num uint32) {
	// int max number
	if num > 2147483647 {
		num = 2147483647
	}
	// 最小値の対応とかも今後していきたい
	bs := make([]byte, 4)
	// int32 を、リトルエンディアンで 16 進数の 4 つの要素に分解
	binary.LittleEndian.PutUint32(bs, num)

	// 指定された箇所に、それぞれの要素を保存
	a.ByteBuffer[offset] = bs[0]
	a.ByteBuffer[offset+1] = bs[1]
	a.ByteBuffer[offset+2] = bs[2]
	a.ByteBuffer[offset+3] = bs[3]
}

// page に引数で渡された string を書き込む
func (a Page) SetString(offset int, str string) {
	bs := []byte(str)
	a.SetBytes(bs, offset)
}

// page に引数の内容を書き込む
func (a *Page) SetBytes(bs []byte, offset int) {
	a.SetInt(offset, uint32(len(bs)))

	// 文字列の長さを最初の 4 bytes = 32 bits で表すため、最終的な長さは offset + 4 byte
	offset += 4
	for index, value := range bs {
		a.ByteBuffer[offset+index] = value
	}
}

// なんだこれ ?
// stringの長さ + 4 bytes (文字列の大きさを表す)
// public static int maxLength(int strlen) の命名を変更した
func MaxLengthOfStringOnPage(str string) int {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return len(str) + 4
}

func MaxLength(strlen int) int {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return strlen + 4
}

// page オブジェクトの中身を返す
func (a Page) Contents() []byte {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return a.ByteBuffer
}

// page オブジェクトを作成する。つまりメモリに領域を確保する。引数の size は原則ブロックサイズと一致する
func CreatePage(size int) Page {
	newBuff := make([]byte, size)
	return Page{ByteBuffer: newBuff}
}

// page オブジェクトを作成する。つまりメモリに領域を確保する。引数の size は原則ブロックサイズと一致する
func CreatePageByBytes(bytes []byte) Page {
	return Page{ByteBuffer: bytes}
}

// buffer を page の単位で扱えるようにする
func LoadBufferToPage(initialBytes []byte) *Page {
	return &Page{ByteBuffer: initialBytes}
}
