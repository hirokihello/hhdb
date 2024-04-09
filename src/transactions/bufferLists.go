package transactions

import (
	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/files"
)

type BufferList struct {
	bufferManager *buffers.Manager
	buffers       map[files.Block]*buffers.Buffer
	pins          map[files.Block]int
}

func (bufferList *BufferList) getBuffer(blk files.Block) *buffers.Buffer {
	buffer := bufferList.buffers[blk]
	return buffer
}

func (bufferList *BufferList) pin(blk files.Block) {
	buffer, _ := bufferList.bufferManager.Pin(&blk)

	bufferList.buffers[blk] = buffer
	bufferList.pins[blk] = 1
}

func (bufferList *BufferList) unPin(blk files.Block) {
	buffer := bufferList.buffers[blk]
	bufferList.bufferManager.UnPin(buffer)
	delete(bufferList.pins, blk)
	res := bufferList.pins[blk]
	if res > 0 {
		delete(bufferList.buffers, blk)
	}
}

func (bufferList *BufferList) unpinAll() {
	for blk := range bufferList.pins {
		buffer := bufferList.buffers[blk]
		bufferList.bufferManager.UnPin(buffer)
	}

	// buffer list を空にする
	for b := range bufferList.buffers {
		delete(bufferList.buffers, b)
	}
	// pins list を空にする
	for b := range bufferList.pins {
		delete(bufferList.pins, b)
	}
}

func CreateBufferList(bufferManager *buffers.Manager) *BufferList {
	return &BufferList{
		bufferManager: bufferManager,
		buffers:       make(map[files.Block]*buffers.Buffer),
		pins:          make(map[files.Block]int),
	}
}
