package records

import (
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/transactions"
)

// そのレコードが使用されているか否かの識別フラグ
const EMPTY = 0
const USED = 1

//slot 番号(そのブロックの中の何番目のレコードか)を使用して操作する。slot 番号は 0 indexed.
type RecordPage struct {
	transaction *transactions.Transaction
	blk         *files.Block
	layout      *Layout
}

func CreateRecordPage(transaction *transactions.Transaction, blk *files.Block, layout *Layout) *RecordPage {
	rec := &RecordPage{
		transaction: transaction,
		blk:         blk,
		layout:      layout,
	}

	// このオブジェクトが生成されるときは必ず transaction 内部で何かしら処理をしたいときなので該当するブロックを pin しておく
	rec.transaction.Pin(*blk)
	return rec
}

func (rec *RecordPage) GetInt(slot int, fieldName string) int {
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	return rec.transaction.GetInt(*rec.blk, fieldPosition)
}

func (rec *RecordPage) GetString(slot int, fieldName string) string {
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	return rec.transaction.GetString(*rec.blk, fieldPosition)
}

func (rec *RecordPage) SetInt(slot int, fieldName string, value int) {
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	rec.transaction.SetInt(*rec.blk, fieldPosition, value, true)
}

func (rec *RecordPage) SetString(slot int, fieldName string, value string) {
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	rec.transaction.SetString(*rec.blk, fieldPosition, value, true)
}

func (rec *RecordPage) Delete(slot int) {
	rec.setFlag(slot, EMPTY)
}

// レコードの中身を全て削除するメソッド。delete all。基本使わない。
func (rec *RecordPage) Format() {
	slot := 0
	for rec.isValidSlot(slot) {
		rec.transaction.SetInt(*rec.blk, rec.offset(slot), EMPTY, false)
		schema := rec.layout.schema

		for field := range schema.fields {
			fieldPos := rec.offset(slot) + rec.layout.offsets[field]

			if schema.Type(field) == INTERGER {
				rec.transaction.SetInt(*rec.blk, fieldPos, 0, false)
			} else {
				rec.transaction.SetString(*rec.blk, fieldPos, "", false)
			}
		}
		slot++
	}
}

func (rec *RecordPage) NextAfter(slot int) int {
	// 使用中の次のスロットを検索する
	return rec.searchAfter(slot, USED)
}

// slot を一つ USED に変更する
func (rec *RecordPage) InsertAfter(slot int) int {
	// 使用していない次のスロットを検索する
	newSlot := rec.searchAfter(slot, EMPTY)
	if newSlot >= 0 {
		rec.setFlag(newSlot, USED)
	}

	return newSlot
}

// 現在の slot よりも後ろを確認していく
func (rec *RecordPage) searchAfter(slot int, flag int) int {
	slot++
	for rec.isValidSlot(slot) {
		if rec.transaction.GetInt(*rec.blk, rec.offset(slot)) == flag {
			return slot
		}
		slot++
	}

	return -1
}

func (rec *RecordPage) setFlag(slot int, flag int) {
	rec.transaction.SetInt(*rec.blk, rec.offset(slot), flag, true)
}

func (rec *RecordPage) offset(slot int) int {
	return slot * rec.layout.slotSize
}

// その slot がブロックの範囲内に収まっているか
func (rec *RecordPage) isValidSlot(slot int) bool {
	// +1 しているのは、slot 自体は offset の位置から開始する。そして終端は offset(slot + 1) になる。
	// このメソッドで判定しているのは終端がブロックに収まっているかどうか
	return rec.offset(slot+1) <= rec.transaction.BlockSize()
}

func (rec *RecordPage) Block() files.Block {
	return *rec.blk
}
