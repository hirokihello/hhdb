package dbFile;

import "testing";
import (
	"encoding/binary"
	"bytes"
)

func TestBlock (t *testing.T) {
	blk := Block{FileName: "testBlockName", BlockNumber: 1};
	if(blk.ToString() != "filename: testBlockName, block: 1") {
		t.Errorf("blk.ToString() = [%v], want: [%v]", blk.ToString(), "filename: testBlockName, block: 1")
	}
}

func TestPage (t *testing.T) {
	page := CreatePage(3);

	if(bytes.Compare(page.ByteBuffer, make([]byte, 3)) != 0) {
		t.Errorf("buffer is not created properly")
	}

	var sample string = "it is for LoadBufferToPage func";
	loadedPage := LoadBufferToPage([]byte(sample));

	if(sample != string(loadedPage.ByteBuffer)) {
		t.Errorf("page was not correctly loaded");
	}
}

func TestPageGetInt (t *testing.T) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 2147483647)
	page := LoadBufferToPage(bs);

	// t.Errorf(string(page.ByteBuffer));
	if(len(page.ByteBuffer) > 4) {
		t.Errorf("length of bytes is[%v]", len(page.ByteBuffer))
	}

	if(int32(binary.LittleEndian.Uint32(page.ByteBuffer)) != 2147483647) {
		t.Errorf("number [%v] is wrong", int32(binary.LittleEndian.Uint64(page.ByteBuffer)) )
	}
}

func TestPgeSetInt (t *testing.T) {
	bs := []byte("fasjfauifnauifnwefbwebfiwbvvfavvdavav");
	page := LoadBufferToPage(bs);

	page.SetInt(1234, 4);

	if(page.GetInt(4) != 1234) {
		t.Errorf("blk.ToString() = [%v], want: [%v]", page.GetInt(4), 1234)
	}
}

func TestPageSetString (t *testing.T) {
	bs := []byte("fasjfauifnauifnwefbwebfiwbvvfavvdavav");
	page := LoadBufferToPage(bs);

	page.SetString("1234ABCD", 4);

	if(page.GetString(4) != "1234ABCD") {
		t.Errorf("blk.ToString() = [%v], want: [%v]", page.GetString(4), "1234ABCD")
	}
}