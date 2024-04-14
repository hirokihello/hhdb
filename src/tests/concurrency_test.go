package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/hirokihello/hhdb/src/db"
	"github.com/hirokihello/hhdb/src/files"
	"github.com/hirokihello/hhdb/src/transactions"
)

func TestConcurrency(t *testing.T) {
	db := db.CreateDB("test_dir_concurrency", 400, 3)
	fileManeger := db.FileManager
	LogManager := db.LogManager
	bufferManager := db.BufferManager

	t.Run("thread a", func(t *testing.T) {
		t.Parallel()
		txA := transactions.CreateTransaction(fileManeger, LogManager, bufferManager)

		blk1 := files.Block{"testfile", 0}
		blk2 := files.Block{"testfile", 1}

		// transaction A の内部で二つの block について slock を獲得する
		txA.Pin(blk1)
		txA.Pin(blk2)

		fmt.Printf("Tx A: request slock 1 \n")
		txA.GetInt(blk1, 0)
		fmt.Printf("Tx A: receive slock 1 \n")
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("Tx A: request slock 2 \n")
		txA.GetInt(blk2, 0)
		fmt.Printf("Tx A: receive slock 2 \n")

		// 二つとも、slock を解放する
		txA.Commit()
		fmt.Printf("Tx A: commit")
	})

	t.Run("thread b", func(t *testing.T) {
		t.Parallel()
		txB := transactions.CreateTransaction(fileManeger, LogManager, bufferManager)

		// A / B / C 同じブロックを参照する
		blk1 := files.Block{"testfile", 0}
		blk2 := files.Block{"testfile", 1}

		// transaction A の内部で二つの block について slock を獲得する
		txB.Pin(blk1)
		txB.Pin(blk2)

		fmt.Printf("Tx B: request xlock 2 \n")
		txB.SetInt(blk2, 0, 0, false)
		fmt.Printf("Tx B: receive xlock 2 \n")
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("Tx B: request slock 1 \n")
		txB.GetInt(blk1, 0)
		fmt.Printf("Tx B: receive slock 1 \n")

		// 二つとも、slock を解放する
		txB.Commit()
		fmt.Printf("Tx B: commit")
	})

	t.Run("thread c", func(t *testing.T) {
		t.Parallel()
		txC := transactions.CreateTransaction(fileManeger, LogManager, bufferManager)

		// A / B / C 同じブロックを参照する
		blk1 := files.Block{"testfile", 0}
		blk2 := files.Block{"testfile", 1}

		// transaction A の内部で二つの block について slock を獲得する
		txC.Pin(blk1)
		txC.Pin(blk2)

		fmt.Printf("Tx C: request xlock 1 \n")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Printf("Tx C: receive xlock 1 \n")
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("Tx C: request slock 2 \n")
		txC.GetInt(blk2, 0)
		fmt.Printf("Tx C: receive slock 2 \n")

		// 二つとも、slock を解放する
		txC.Commit()
		fmt.Printf("Tx C: commit")
	})
}
