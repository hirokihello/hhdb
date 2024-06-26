package tests
import "github.com/hirokihello/hhdb/src/files"

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBlock(t *testing.T) {
	blk := files.Block{FileName: "testBlockName", Number: 1}
	if blk.ToString() != "filename: testBlockName, block: 1" {
		t.Errorf("blk.ToString() = [%v], want: [%v]", blk.ToString(), "filename: testBlockName, block: 1")
	}
}

func TestPage(t *testing.T) {
	page := files.CreatePage(3)

	if bytes.Compare(page.ByteBuffer, make([]byte, 3)) != 0 {
		t.Errorf("buffer is not created properly")
	}

	var sample string = "it is for LoadBufferToPage func"
	loadedPage := files.LoadBufferToPage([]byte(sample))

	if sample != string(loadedPage.ByteBuffer) {
		t.Errorf("page was not correctly loaded")
	}
}

func TestPageGetInt(t *testing.T) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 2147483647)
	page := files.LoadBufferToPage(bs)

	if len(page.ByteBuffer) > 4 {
		t.Errorf("length of bytes is[%v]", len(page.ByteBuffer))
	}

	if int32(binary.LittleEndian.Uint32(page.ByteBuffer)) != 2147483647 {
		t.Errorf("number [%v] is wrong", int32(binary.LittleEndian.Uint64(page.ByteBuffer)))
	}
}

func TestPgeSetInt(t *testing.T) {
	bs := []byte("fasjfauifnauifnwefbwebfiwbvvfavvdavav")
	page := files.LoadBufferToPage(bs)

	page.SetInt(4, 1234)

	if page.GetInt(4) != 1234 {
		t.Errorf("blk.ToString() = [%v], want: [%v]", page.GetInt(4), 1234)
	}
}

func TestPageSetString(t *testing.T) {
	bs := []byte("fasjfauifnauifnwefbwebfiwbvvfavvdavav")
	page := files.LoadBufferToPage(bs)

	page.SetString(4, "1234ABCD")

	if page.GetString(4) != "1234ABCD" {
		t.Errorf("blk.ToString() = [%v], want: [%v]", page.GetString(4), "1234ABCD")
	}
}

func TestManager(t *testing.T) {
	file_manager := files.CreateManager("test_dir", 400)
	block := files.Block{FileName: "test_block_2", Number: 2}
	page_1 := files.CreatePage(400)
	position_1 := 88
	page_1.SetString(position_1, "abcdefggg")
	size := files.MaxLengthOfStringOnPage("abcdefggg")
	position_2 := size + position_1
	page_1.SetInt(position_2, 345)
	file_manager.Write(block, page_1)

	page_2 := files.CreatePage(400)
	file_manager.Read(block, page_2)

	if page_1.GetInt(position_2) != 345 {
		t.Errorf("page_1.GetInt(position_2) = [%v], want: [%v]", page_1.GetInt(position_2), "345")
	}
	if page_1.GetString(position_1) != "abcdefggg" {
		t.Errorf("page_1.GetString(position_1) = [%v], want: [%v]", page_1.GetString(position_2), "abcdefggg")
	}

	// page2についてのテストケースの追加
	if page_2.GetInt(position_2) != 345 {
		t.Errorf("page_2.GetInt(position_2) = [%v], want: [%v]", page_2.GetInt(position_2), "345")
	}
	if page_2.GetString(position_1) != "abcdefggg" {
		t.Errorf("page_2.GetString(position_1) = [%v], want: [%v]", page_2.GetString(position_1), "abcdefggg")
	}
}
