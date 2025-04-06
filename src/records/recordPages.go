package records

import (
	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/transactions"
)

// そのレコードが使用されているか否かの識別フラグ
const EMPTY = 0
const USED = 1

// slot 番号(そのブロックの中の何番目のレコードか)を使用して操作する。slot 番号は 0 indexed.
type RecordPage struct {
	transaction *transactions.Transaction
	blk         *files.Block
	layout      *Layout
}

// record page オブジェクトを作成する。引数で受け取った block を pin して buffer 上に読み込んで操作できるようにする
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

// フィールド名とスロットから、そのスロットのフィールドを取得
func (rec *RecordPage) GetInt(slot int, fieldName string) int {
	// 「slot の offset 分 + それぞれのレコード内のフィールドのオフセット」の位置を算出(そこにそのフィールドの値が入っている)
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	return rec.transaction.GetInt(*rec.blk, fieldPosition)
}

// フィールド名とスロットから、そのスロットのフィールドを取得
func (rec *RecordPage) GetString(slot int, fieldName string) string {
	// 「slot の offset 分 + それぞれのレコード内のフィールドのオフセット」の位置を算出(そこにそのフィールドの値が入っている)
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	return rec.transaction.GetString(*rec.blk, fieldPosition)
}

// フィールド名とスロットから、そのスロットのフィールドを更新
func (rec *RecordPage) SetInt(slot int, fieldName string, value int) {
	// 「slot の offset 分 + それぞれのレコード内のフィールドのオフセット」の位置を算出(そこにそのフィールドの値が入っている)
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	rec.transaction.SetInt(*rec.blk, fieldPosition, value, true)
}

// フィールド名とスロットから、そのスロットのフィールドを更新
func (rec *RecordPage) SetString(slot int, fieldName string, value string) {
	// 「slot の offset 分 + それぞれのレコード内のフィールドのオフセット」の位置を算出(そこにそのフィールドの値が入っている)
	fieldPosition := rec.offset(slot) + rec.layout.Offset(fieldName)
	rec.transaction.SetString(*rec.blk, fieldPosition, value, true)
}

// slot を空にする
func (rec *RecordPage) Delete(slot int) {
	rec.setFlag(slot, EMPTY)
}

// record として使用できるように、そのブロックの内容を初期状態にするメソッド
func (rec *RecordPage) Format() {
	slot := 0
	for rec.isValidSlot(slot) {
		rec.transaction.SetInt(*rec.blk, rec.offset(slot), EMPTY, false)
		schema := rec.layout.schema

		for _, field := range schema.Fields() {
			fieldPos := rec.offset(slot) + rec.layout.offsets[field]

			if schema.Type(field) == consts.INTEGER {
				rec.transaction.SetInt(*rec.blk, fieldPos, 0, false)
			} else {
				rec.transaction.SetString(*rec.blk, fieldPos, "", false)
			}
		}
		slot++
	}
}

// 現在の位置以降で使用している同じブロックの最初のスロットを返す。ない場合 -1 を返す
func (rec *RecordPage) NextAfter(slot int) int {
	return rec.searchAfter(slot, USED)
}

// 現在の位置以降で使用していない、同じブロックの現在のスロットから見て次のスロットを一つ USED に変更する。できない場合は -1 を返す
func (rec *RecordPage) InsertAfter(slot int) int {
	// 使用していない次のスロットを検索する
	newSlot := rec.searchAfter(slot, EMPTY)
	if newSlot >= 0 {
		rec.setFlag(newSlot, USED)
	}

	return newSlot
}

// ブロック内の現在の slot よりも後ろにある slot を検索する。flag に一致するものを検索。存在しない場合、-1 を返却する
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

// slot(何スロット目) * rec.layout.slotSize(1 スロットあたりのサイズ)
func (rec *RecordPage) offset(slot int) int {
	return slot * rec.layout.slotSize
}

// その slot のサイズがブロックのサイズに収まっているか
func (rec *RecordPage) isValidSlot(slot int) bool {
	// +1 しているのは、slot 自体は offset の位置から開始する。そして終端は offset(slot + 1) になる。
	// このメソッドで判定しているのは終端がブロックに収まっているかどうか
	return rec.offset(slot+1) <= rec.transaction.BlockSize()
}

func (rec *RecordPage) Block() files.Block {
	return *rec.blk
}
