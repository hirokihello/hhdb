package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/transactions"
)

func TestTransaction(t *testing.T) {
	testDb := db.CreateDB("test_dir_transactions", 400, 3)
	testLogManager := testDb.LogManager
	testFileManager := testDb.FileManager
	testBufferManager := testDb.BufferManager

	tx1 := transactions.CreateTransaction(testFileManager, testLogManager, testBufferManager)
	blk := files.Block{
		FileName: "transactionTestFile",
		Number:   0,
	}
	tx1.Pin(blk)

	// transaction 1 で無事に書き込みができるかのテスト
	TX1_SAMPLE_VAR1 := 1
	TX1_SAMPLE_VAR2 := "transactionTest 1 dayo"
	tx1.SetInt(blk, 80, TX1_SAMPLE_VAR1, false)
	tx1.SetString(blk, 40, TX1_SAMPLE_VAR2, false)

	fmt.Print("\nTransaction current value\n")
	fmt.Print(tx1.GetInt(blk, 80)) //=> 先ほど書き込んだ 1 が返ってくるはず
	fmt.Print("\n")
	fmt.Print(tx1.GetString(blk, 40)) // => 先ほど書き込んだ "transactionTest dayo" が返ってくるはず
	fmt.Print("\nTransaction current value end\n")

	tx1.Commit()

	// // 二つ目の transaction を作成して、先ほどの transaction の結果を先ほどの block を参照することで確認する
	tx2 := transactions.CreateTransaction(testFileManager, testLogManager, testBufferManager)
	tx2.Pin(blk)

	// transaction 2 で無事に書き込みができるかのテスト
	TX2_SAMPLE_VAR1 := 2
	TX2_SAMPLE_VAR2 := "transactionTest 2 dayo!!!!"
	tx2.SetInt(blk, 80, TX2_SAMPLE_VAR1, true)
	tx2.SetString(blk, 40, TX2_SAMPLE_VAR2, true)

	fmt.Println("tx2.GetInt(blk, 80):", tx2.GetInt(blk, 80))       //=> 先ほど書き込んだ 1 が返ってくるはず
	fmt.Println("tx2.GetString(blk, 40):", tx2.GetString(blk, 40)) // => 先ほど書き込んだ "transactionTest dayo" が返ってくるはず

	tx2.Commit()

	// 3 つ目の transaction を作成して、先ほどの transaction の結果を先ほどの block を参照することで確認する
	tx3 := transactions.CreateTransaction(testFileManager, testLogManager, testBufferManager)
	tx3.Pin(blk)

	TX3_SAMPLE_VAR1 := 3333
	TX3_SAMPLE_VAR2 := "transactionTest 3 dayo!!!!"
	tx3.SetInt(blk, 80, TX3_SAMPLE_VAR1, true)
	tx3.SetString(blk, 40, TX3_SAMPLE_VAR2, true)
	fmt.Println("tx3.GetInt(blk, 80):", tx3.GetInt(blk, 80))
	fmt.Println("tx3.GetString(blk, 40):", tx3.GetString(blk, 40))
	tx3.Rollback()

	// 4 つ目の transaction でも rollback がうまくいっていることを確認する
	tx4 := transactions.CreateTransaction(testFileManager, testLogManager, testBufferManager)
	tx4.Pin(blk)

	fmt.Println()
	fmt.Println("tx4.GetInt(blk, 80):", tx4.GetInt(blk, 80))
	fmt.Println("tx4.GetString(blk, 40):", tx4.GetString(blk, 40))

	// pos 80 は transaction 2 で設定した値と一致しているはず
	if TX2_SAMPLE_VAR1 != tx4.GetInt(blk, 80) {
		t.Errorf("[" + strconv.Itoa(tx4.GetInt(blk, 80)) + "] is not correct. expected: " + strconv.Itoa(TX2_SAMPLE_VAR1))
	}
	// pos 40 は transaction 2 で設定した値と一致しているはず
	if TX2_SAMPLE_VAR2 != tx4.GetString(blk, 40) {
		t.Errorf("[" + tx4.GetString(blk, 40) + "] is not correct. expected: " + TX2_SAMPLE_VAR2)
	}
	tx4.Rollback()
}
