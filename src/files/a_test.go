package files;

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

func TestManager (t *testing.T) {
	file_manager := CreateManager("test_dir", 400);
	block := Block{FileName: "test_block_2", BlockNumber: 2};
	page_1 := CreatePage(400);
	position_1 := 88;
	page_1.SetString("abcdefggg", position_1);
	size := page_1.MaxLength("abcdefggg");
	position_2 := size + position_1;
	page_1.SetInt(345, position_2);
	file_manager.Write(block, page_1);

	page_2 := CreatePage(400);
	file_manager.Read(block, page_2);

	if(page_1.GetInt(position_2) != 345) {
		t.Errorf("page_1.GetInt(position_2) = [%v], want: [%v]", page_1.GetInt(position_2), "345")
	}
	if(page_1.GetString(position_1) != "abcdefggg") {
		t.Errorf("page_1.GetString(position_1) = [%v], want: [%v]", page_1.GetString(position_2), "abcdefggg")
	}

	// page2についてのテストケースの追加
	if(page_2.GetInt(position_2) != 345) {
		t.Errorf("page_2.GetInt(position_2) = [%v], want: [%v]", page_2.GetInt(position_2), "345")
	}
	if(page_2.GetString(position_1) != "abcdefggg") {
		t.Errorf("page_2.GetString(position_1) = [%v], want: [%v]", page_2.GetString(position_1), "abcdefggg")
	}
}