package records

import (
	"github.com/hirokihello/hhdb/src/consts"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/queries"
	"github.com/hirokihello/hhdb/src/transactions"
)

// テーブルの値を読み取っていい感じに扱えるようにした
type TableScan struct {
	transaction *transactions.Transaction
	layout      *Layout
	recordPage  *RecordPage
	fileName    string
	currentSlot int
}

func CreateTableScan(transaction *transactions.Transaction, tableName string, layout *Layout) *TableScan {
	tableScan := TableScan{
		transaction: transaction,
		fileName:    tableName + ".tbl",
		layout:      layout,
	}

	//初期状態で 1 つのブロックを作成するように変更されてしまっているので、その仕様に合わせた変更....
	if transaction.Size(tableScan.fileName) == 0 {
		tableScan.moveToNewBlock()
	} else {
		tableScan.moveToBlock(0)
	}

	return &tableScan
}

func (tableScan *TableScan) Close() {
	if tableScan.recordPage != nil {
		tableScan.transaction.UnPin(*tableScan.recordPage.blk)
	}
}

// ファイルの最初に移動する
func (tableScan *TableScan) BeforeFirst() {
	tableScan.moveToBlock(0)
}

func (tableScan *TableScan) Next() bool {
	tableScan.currentSlot = tableScan.recordPage.NextAfter(tableScan.currentSlot)
	// 空いているスロットが現在のブロックにない場合
	for tableScan.currentSlot < 0 {
		// 今いるのが最終ブロックの場合 (最後のブロックの最後のレコードのスロットを現在見ている場合)
		if tableScan.atLastBlock() {
			return false
		}
		// そうではない場合 (まだ次のブロックがファイルに存在する場合)
		// 次のブロックに移動する
		tableScan.moveToBlock(tableScan.recordPage.blk.Number + 1)
		// slot 番号を更新する
		tableScan.currentSlot = tableScan.recordPage.NextAfter(tableScan.currentSlot)
	}

	return true
}

// 現在見ているスロットの中身を返却する
func (tableScan *TableScan) GetInt(fieldName string) int {
	return tableScan.recordPage.GetInt(tableScan.currentSlot, fieldName)
}

// 現在見ているスロットの中身を返却する
func (tableScan *TableScan) GetString(fieldName string) string {
	return tableScan.recordPage.GetString(tableScan.currentSlot, fieldName)
}

// 現在見ているスロットの中身を返却する
func (tableScan *TableScan) GetValue(fieldName string) *queries.Constants {
	if tableScan.layout.Schema().Type(fieldName) == consts.INTEGER {
		return queries.CreateConstantByInt(tableScan.GetInt(fieldName))
	} else {
		return queries.CreateConstantByString(tableScan.GetString(fieldName))
	}
}
func (tableScan *TableScan) HasField(FieldName string) bool {
	return tableScan.layout.Schema().hasField(FieldName)
}

// 現在のスロットのフィールドの値を更新
func (tableScan *TableScan) SetInt(fieldName string, value int) {
	tableScan.recordPage.SetInt(tableScan.currentSlot, fieldName, value)
}

// 現在のスロットのフィールドの値を更新
func (tableScan *TableScan) SetString(fieldName string, value string) {
	tableScan.recordPage.SetString(tableScan.currentSlot, fieldName, value)
}

// fieldName と queries.Constants を受け取ってよしなに更新する
func (tableScan *TableScan) SetVal(fieldName string, value queries.Constants) {
	if tableScan.layout.Schema().Type(fieldName) == consts.INTEGER {
		tableScan.SetInt(fieldName, value.AsInt())
	} else {
		tableScan.SetString(fieldName, value.AsString())
	}
}

// 現在の slot の次に空いている slot を USED に変更する。そしてそこに current slot を変更する
func (tableScan *TableScan) Insert() {
	// ブロック内に空きがあれば更新処理
	tableScan.currentSlot = tableScan.recordPage.InsertAfter(tableScan.currentSlot)
	// 空いているスロットが現在のブロックになくて更新できなかった場合
	for tableScan.currentSlot < 0 {
		// 今いるのが最終ブロックの場合 (最後のブロックの最後のレコードのスロットを現在見ている場合)
		if tableScan.atLastBlock() {
			// 新しいブロックを作成して、そこに移る
			tableScan.moveToNewBlock()
		} else {
			// そうではない場合 (まだ次のブロックがファイルに存在する場合)
			// 次のブロックに移動する
			tableScan.moveToBlock(tableScan.recordPage.blk.Number + 1)
		}
		// slot 番号を更新する
		tableScan.currentSlot = tableScan.recordPage.InsertAfter(tableScan.currentSlot)
	}
}

func (tableScan *TableScan) Delete() {
	tableScan.recordPage.Delete(tableScan.currentSlot)
}

func (tableScan *TableScan) MoveToRid(rid Rid) {
	tableScan.Close()

	blk := files.Block{FileName: tableScan.fileName, Number: rid.BlockNumber()}
	tableScan.recordPage = CreateRecordPage(
		tableScan.transaction,
		&blk,
		tableScan.layout,
	)
	tableScan.currentSlot = rid.Slot()
}

func (tableScan *TableScan) GetRid() *Rid {
	return createRid(tableScan.recordPage.blk.Number, tableScan.currentSlot)
}

func (tableScan *TableScan) moveToBlock(blockNumber int) {
	tableScan.Close()
	blk := files.Block{FileName: tableScan.fileName, Number: blockNumber}
	// 見ている page を移動する
	tableScan.recordPage = CreateRecordPage(tableScan.transaction, &blk, tableScan.layout)
	// ブロックを移動したので、位置を -1 にしておく
	tableScan.currentSlot = -1
}

func (tableScan *TableScan) moveToNewBlock() {
	tableScan.Close()

	// 新しく transaction により block を作成する
	blk := tableScan.transaction.Append(tableScan.fileName)

	// 見ている page を移動する
	tableScan.recordPage = CreateRecordPage(tableScan.transaction, blk, tableScan.layout)
	tableScan.recordPage.Format()
	// ブロックを移動したので、位置を -1 にしておく
	tableScan.currentSlot = -1
}

func (tableScan *TableScan) atLastBlock() bool {
	return tableScan.recordPage.Block().Number == tableScan.transaction.Size(tableScan.fileName)-1
}
