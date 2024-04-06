package tests

import (
	"fmt"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/transactions"
)

func TestTransaction(t *testing.T) {
	testDb := db.CreateDB("test_dir", 400, 3)
	testLogManager := testDb.LogManager
	testFileManager := testDb.FileManager
	testBufferManager := testDb.BufferManager

	tx1 := transactions.CreateTransaction(testLogManager, testFileManager, testBufferManager)
	blk := files.Block{
		FileName: "transactionTestFile",
		Number:   1,
	}
	tx1.pin(blk)

	// transaction 1 で無事に書き込みができるかのテスト
	TX1_SAMPLE_VAR1 := 1
	TX1_SAMPLE_VAR2 := "transactionTest dayo"
	tx1.SetInt(blk, 80, TX1_SAMPLE_VAR1, false)
	tx1.SetString(blk, 40, TX1_SAMPLE_VAR2, false)
	tx1.Commit()

	// 二つ目の transaction を作成して、先ほどの transaction の結果を先ほどの block を参照することで確認する
	tx2 := transactions.CreateTransaction(testLogManager, testFileManager, testBufferManager)
	tx2.pin(blk)

	tx2.GetInt(blk, 80)    //=> 先ほど書き込んだ 1 が返ってくるはず
	tx2.GetString(blk, 40) // => 先ほど書き込んだ "transactionTest dayo" が返ってくるはず
	if TX1_SAMPLE_VAR1 != tx2.GetInt(blk, 80) {
		t.Errorf("[ " + tx2.GetInt(blk, 80) + "] is not correct. expected: " + TX1_SAMPLE_VAR1)
	}

	if TX1_SAMPLE_VAR1 != tx2.GetString(blk, 40) {
		t.Errorf("[ " + tx2.GetInt(blk, 40) + "] is not correct. expected: " + TX1_SAMPLE_VAR1)
	}

	// transaction 2 で無事に書き込みができるかのテスト
	TX2_SAMPLE_VAR1 := 2
	TX2_SAMPLE_VAR2 := "transactionTest dayo1!!!!"
	tx2.SetInt(blk, 80, TX2_SAMPLE_VAR1, false)
	tx2.SetString(blk, 40, TX2_SAMPLE_VAR2, false)
	tx2.Commit()

	if TX2_SAMPLE_VAR1 != tx2.GetInt(blk, 80) {
		t.Errorf("[ " + tx2.GetInt(blk, 80) + "] is not correct. expected: " + TX2_SAMPLE_VAR1)
	}

	if TX2_SAMPLE_VAR2 != tx2.GetString(blk, 40) {
		t.Errorf("[ " + tx2.GetInt(blk, 40) + "] is not correct. expected: " + TX2_SAMPLE_VAR2)
	}

	// 3 つ目の transaction を作成して、先ほどの transaction の結果を先ほどの block を参照することで確認する
	tx3 := transactions.CreateTransaction(testLogManager, testFileManager, testBufferManager)
	tx3.pin(blk)

	TX3_SAMPLE_VAR1 := 3333
	TX3_SAMPLE_VAR2 := "transactionTest 3 dayo!!!!"
	tx3.SetInt(blk, 80, TX3_SAMPLE_VAR1, false)
	tx3.SetString(blk, 40, TX3_SAMPLE_VAR2, false)

	fmt.Print("tx3 の rollback 前の値")
	fmt.Print(tx2.GetInt(blk, 80))
	fmt.Print(tx2.GetString(blk, 40))
	tx3.GetInt(blk, 80)    //=> 先ほど書き込んだ 1 が返ってくるはず
	tx3.GetString(blk, 40) // => 先ほど書き込んだ "transactionTest dayo" が返ってくるはず
	tx3.Rollback()

	fmt.Print("tx3 の rollback 後の値")
	fmt.Print(tx3.GetInt(blk, 80))
	fmt.Print(tx3.GetString(blk, 40))

	// pos 80 は transaction 2 で設定した値と一致しているはず
	if TX2_SAMPLE_VAR2 != tx3.GetInt(blk, 80) {
		t.Errorf("[ " + tx3.GetInt(blk, 80) + "] is not correct. expected: " + TX2_SAMPLE_VAR1)
	}
	// pos 40 は transaction 2 で設定した値と一致しているはず
	if TX2_SAMPLE_VAR2 != tx3.GetString(blk, 40) {
		t.Errorf("[ " + tx3.GetInt(blk, 40) + "] is not correct. expected: " + TX2_SAMPLE_VAR1)
	}

	// 4 つ目の transaction でも rollback がうまくいっていることを確認する
	tx4 := transactions.CreateTransaction(testLogManager, testFileManager, testBufferManager)
	// pos 80 は transaction 2 で設定した値と一致しているはず
	if TX2_SAMPLE_VAR2 != tx4.GetInt(blk, 80) {
		t.Errorf("[ " + tx4.GetInt(blk, 80) + "] is not correct. expected: " + TX2_SAMPLE_VAR1)
	}
	// pos 40 は transaction 2 で設定した値と一致しているはず
	if TX2_SAMPLE_VAR2 != tx4.GetString(blk, 40) {
		t.Errorf("[ " + tx4.GetInt(blk, 40) + "] is not correct. expected: " + TX2_SAMPLE_VAR1)
	}

}
