package dbFile;

import "testing";
import ("bytes");

func TestBlock (t *testing.T) {
	blk := Block{FileName: "testBlockName", BlockNumber: 1};
	// blk := CreateBlock("testBlockName", 1);
	if(blk.ToString() != "filename: testBlockName, block: 1") {
		t.Errorf("blk.ToString() = [%v], want: [%v]", blk.ToString(), "filename: testBlockName, block: 1")
	}
}

// func TestPage (t *testing.T) {
// 	blk := Block{FileName: "testBlockName", BlockNumber: 1};
// 	// blk := CreateBlock("testBlockName", 1);
// 	if(blk.ToString() != "filename: testBlockName, block: 1") {
// 		t.Errorf("blk.ToString() = [%v], want: [%v]", blk.ToString(), "filename: testBlockName, block: 1")
// 	}
// }

func TestPage (t *testing.T) {
	page := CreatePage(3);

	if(page.ByteBuffer.String()!= bytes.NewBuffer(make([]byte, 3)).String()) {
		t.Errorf("buffer is not created properly")
	}

	var sample string = "it is for LoadBufferToPage func";
	loadedPage := LoadBufferToPage([]byte(sample));

	if(sample != loadedPage.ByteBuffer.String()) {
		t.Errorf("page was not correctly loaded");
	}
}