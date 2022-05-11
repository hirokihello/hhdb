package dbFile
import ("bytes");
// import (
// 	"unicode/utf8"
// )
// 文字こーどの現状での必要性がなさそうなので放置

type Page struct {
	ByteBuffer bytes.Buffer;
}

func getString (a []byte, offset int) string {
	bytes := a[offset:];
	return string(bytes);
}

func CreatePage (size int) Page {
	newBuff := bytes.NewBuffer(make([]byte, size));
	return Page{ByteBuffer: *newBuff};
}

func LoadBufferToPage (initialBytes []byte) Page {
	newBuff := bytes.NewBuffer(initialBytes);
	return Page{ByteBuffer: *newBuff}
}