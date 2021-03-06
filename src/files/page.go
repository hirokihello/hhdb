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

type Page struct {
	ByteBuffer []byte;
}

// マジックナンバー４使いがち...

func (a *Page) GetBytes (offset int) []byte {
	size := a.GetInt(offset);
	start := offset+4;
	return a.ByteBuffer[start:start+size]
}

func (a Page) GetString (offset int) string {
	buf := a.GetBytes(offset)
	return string(buf);
}

func (a Page) GetInt (offset int) int {
	return int(binary.LittleEndian.Uint32(a.ByteBuffer[offset:offset+4]));
}

func (a Page) SetInt (num uint32, offset int) {
	// int max number
	if(num > 2147483647) {
		num = 2147483647;
	}
	// 最小値の対応とかも今後していきたい
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, num)

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
	a.SetInt(uint32(len(bs)), offset);

	offset += 4;
	for index, value := range bs {
		a.ByteBuffer[offset+index] = value;
	}
}

// stringの長さ自体を返す
// public static int maxLength(int strlen) の命名を変更した
func MaxLengthOfStringOnPage (str string) int {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return len(str) + 4;
}

func (a Page) Contents () []byte {
	// utf8で実装しておりアルファベットとintのみ受け付ける予定なので現状これで良い。
	return a.ByteBuffer;
}

func CreatePage (size int) Page {
	newBuff := make([]byte, size);
	return Page{ByteBuffer: newBuff};
}

func LoadBufferToPage (initialBytes []byte) Page {
	return Page{ByteBuffer: initialBytes}
}