package tests

import (
	"testing"

	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
)

func TestBufferManager(t *testing.T) {
	db := db.CreateDB("test_dir", 400, 3)
	bufferManager := db.BufferManager

	buffers := make([]*buffers.Buffer, 6)
	buffers[0], _ = bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 0})
	buffers[1], _ = bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 1})
	buffers[2], _ = bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 2})

	bufferManager.Unpin(buffers[1])
	buffers[1] = nil

	buffers[3], _ = bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 0})
	buffers[4], _ = bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 1})

	// 三つ全てのバッファーを使用している
	if bufferManager.Available() != 0 {
		t.Errorf("bufferManager.Available() = [%v], want: [%v]", bufferManager.Available(), "0")
	}

	// 三つ pin されている状態だとエラーが返される
	_, err := bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 3})

	if err == nil {
		t.Errorf("buffer is nil, want not nil")
	}

	bufferManager.Unpin(buffers[2])
	buffers[2] = nil

	_, err2 := bufferManager.Pin(&files.Block{FileName: "buffer_manager_test", Number: 3})

	if err2 != nil {
		t.Errorf("buffer is not nil, want nil")
	}
}

func TestBuffer(t *testing.T) {
	db := db.CreateDB("test_dir", 400, 3)
	bufferManager := db.BufferManager

	buffer1, _ := bufferManager.Pin(&files.Block{FileName: "buffer_test", Number: 0})
	page := buffer1.Contents()
	n := page.GetInt(80)

	if n == 10000000 {
		t.Errorf("page was modified")
	}

	page.SetInt(80, uint32(n+1))

	buffer1.SetModified(1, 0)
	bufferManager.Unpin(buffer1)

	// 三つ全てのバッファーを使用している
	if page.GetInt(80) != n+1 {
		t.Errorf("page.GetInt(80) = [%v], want: [%v]", page.GetInt(80), n+1)
	}

	buffer2, _ := bufferManager.Pin(&files.Block{FileName: "buffer_test", Number: 1})
	bufferManager.Pin(&files.Block{FileName: "buffer_test", Number: 2})
	bufferManager.Pin(&files.Block{FileName: "buffer_test", Number: 3})

	bufferManager.Unpin(buffer2)

	buffer2, _ = bufferManager.Pin(&files.Block{FileName: "buffer_test", Number: 0})
	page2 := buffer2.Contents()
	page2.SetInt(80, uint32(10000000))
	buffer2.SetModified(1, 0)
	bufferManager.Unpin(buffer2)
}
