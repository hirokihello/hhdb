package files
import (
	"encoding/binary"
)
type PageI interface {
	SetString();
	SetInt();
	GetInt();
	GetString();
	Contents();
}

// page を byte の集合として考える
type Page struct {
	ByteBuffer []byte;
}


// 1 byte
func (a *Page) GetBytes (offset int) []byte {
	// 最初の要素に長さが入っているため、長さを取得
	size := a.GetInt(offset);
	// 最初の要素は 4 byte 分なので、その次が最初の要素となる
	start := offset+4;
	return a.ByteBuffer[start:start+size]
}

// offset で渡された箇所を int で読み込み、offset の位置からさらにその読み込まれた int 分の長さを読み込む
func (a Page) GetString (offset int) string {
	buf := a.GetBytes(offset)
	return string(buf);
}

func (a Page) GetInt (offset int) int {
	return int(binary.LittleEndian.Uint32(a.ByteBuffer[offset:offset+4]));
}

// int は 4bytes で保存する。
func (a Page) SetInt (offset int, num uint32) {
	// int max number
	if(num > 2147483647) {
		num = 2147483647;
	}
	// 最小値の対応とかも今後していきたい
	bs := make([]byte, 4)
	// int32 を、リトルエンディアンで 16 進数の 4 つの要素に分解
	binary.LittleEndian.PutUint32(bs, num)

	// 指定された箇所に、それぞれの要素を保存
	a.ByteBuffer[offset] = bs[0];
	a.ByteBuffer[offset+1] = bs[1];
	a.ByteBuffer[offset+2] = bs[2];
	a.ByteBuffer[offset+3] = bs[3];
}

func (a Page) SetString (str string, offset int) {
	bs := []byte(str);
	a.SetBytes(bs, offset);
}

func (a *Page) SetBytes(bs []byte, offset int) {
	a.SetInt(offset, uint32(len(bs)));

	// 文字列の長さを最初の 4 bytes = 32 bits で表すため、最終的な長さは offset + 4 byte
	offset += 4;
	for index, value := range bs {
		a.ByteBuffer[offset+index] = value;
	}
}

// stringの長さ + 4 bytes (文字列の大きさを表す)
// public static int maxLength(int strlen) の命名を変更した
func MaxLengthOfStringOnPage (str string) int {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return len(str) + 4;
}

func (a Page) Contents () []byte {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return a.ByteBuffer;
}

// ブロックのサイズを受け取ることがメインで想定されている
func CreatePage (size int) Page {
	newBuff := make([]byte, size);
	return Page{ByteBuffer: newBuff};
}

// buffer を page の単位で扱えるようにする
func LoadBufferToPage (initialBytes []byte) Page {
	return Page{ByteBuffer: initialBytes}
}