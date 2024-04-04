package files

import (
	"fmt"
	"os"
	"sync"
)

type Manager struct {
	ManagerI
	DbDirectory string // create されるときに引数で渡される
	BlockSize   int    // 1 block の byte 数
	OpenFiles   map[string]*os.File // manager が現在開いている(メモリ上に保持している)ファイル(ブロック)の一覧。ページオブジェクトの一覧。
	mu          sync.Mutex // mutex guards
}

// データベース・エンジンのうち、オペレーティング・システムとやりとりするようなオブジェクト
type ManagerI interface {
	DbDirectory() string // create されるときに引数で渡される
	BlockSize()   int    // 1 block の byte 数
	OpenFiles()   map[string]*os.File // manager が現在開いている(メモリ上に保持している)ファイル(ブロック)の一覧。ページオブジェクトの一覧。

	GetFile(fileName string) *os.File // ファイルを読み込み、プログラム上で使用できるようにする
	Read(blk Block, page Page) // GetFile で読み込んだ物理的なファイルをメモリ上の Page オブジェクトに読み込ませる
	Write(blk Block, page Page) // メモリ上の Page オブジェクトの内容を、対応する物理的なファイルに書き込む。該当のファイルがない場合は作成される
	FileBlockLength(fileName string) int // ファイルに含まれるブロック数を返す
	Append(fileName string) *Block // 新しいファイルを作成する。そのファイルに該当するブロック情報を return する
}

// ファイルをメモリに読み込み、プログラム上で使用できるようにする
func (manager *Manager) GetFile(fileName string) *os.File {
	// ここで使ってるファイルは基本的にblkやpageの単位のファイルと異なる....!!
	// blkやpageがどのように作られるのかはまた別の話
	f, err := manager.OpenFiles[fileName]
	if !err {
		file, err2 := os.OpenFile(manager.DbDirectory+"/"+fileName, os.O_SYNC|os.O_RDWR, 0755)
		if err2 != nil {
			newFile, _ := os.Create(manager.DbDirectory + "/" + fileName)
			file = newFile
			file.Write(make([]byte, manager.BlockSize))
			file.Sync()
		}
		file.Seek(0, 0)
		file.Sync()
		manager.OpenFiles[fileName] = file
		f = file
	}

	return f
}

// 物理的なファイルをメモリ上の Page オブジェクトに読み込ませる
func (manager *Manager) Read(blk Block, page Page) {
	manager.mu.Lock()
	file := manager.GetFile(blk.FileName)
	info, _ := file.Stat()
	if int(info.Size()) < manager.BlockSize*(blk.Number+1) {
		file.Truncate(int64((blk.Number + 1) * manager.BlockSize))
	}

	n, err := file.Seek(int64(blk.Number*manager.BlockSize), 0)
	if err != nil {
		fmt.Println(n)
		fmt.Println("when file.Seek(int64(blk.Number * manager.BlockSize), 0) was occured, error generated: ")
	}

	// page size 分のものを読み込む
	read_n, err := file.Read(page.Contents())
	if err != nil {
		fmt.Println(read_n)
		fmt.Println(err)
		fmt.Println("file.Read(page.Contents());was occured, error occured: ")
	}
	file.Sync()
	manager.mu.Unlock()
}

// メモリ上の Page オブジェクトの内容を、対応する物理的なファイルに書き込む
// 該当のファイルがない場合は作成される
func (manager *Manager) Write(blk Block, page Page) {
	manager.mu.Lock()
	// ブロックの情報からファイルを取得
	file := manager.GetFile(blk.FileName)
	info, _ := file.Stat()
	//ファイルが小さかったら拡張
	if info.Size() < int64((blk.Number + 1) * manager.BlockSize) {
		file.Truncate(int64((blk.Number + 1) * manager.BlockSize))
	}
	// 第二引数0はファイルの先頭からのoffsetを示す
	file.Seek(int64(blk.Number * manager.BlockSize), 0)

	// ページにロードされている内容を読み込む
	file.Write(page.Contents())
	file.Sync()
	manager.mu.Unlock()
}

// ファイルに含まれるブロック数を返す
func (manager *Manager) FileBlockLength(fileName string) int {
	file := manager.GetFile(fileName)
	info, _ := file.Stat()

	return int(int(info.Size()) / manager.BlockSize)
}

// 新しいファイルを作成する
// そのファイルに該当するブロック情報を return する
func (manager *Manager) Append(fileName string) *Block {
	manager.mu.Lock()
	file := manager.GetFile(fileName)

	// ファイルに含まれるブロックの数を返す。
	blockNumber := manager.FileBlockLength(fileName)

	// lengthが現時点の最終ブロック+1 のblockNumberであるので、新規作成で作るのはそのファイルのlengthを直接入れれば良い
	block := Block{FileName: fileName, Number: blockNumber}

	// 新しくブロックを追加するため、blockNumber + 1 となるように、ファイルを拡張しておく」
	file.Truncate(int64((blockNumber + 1) * manager.BlockSize))

	file.Sync()
	manager.mu.Unlock()
	return &block
}

// FileManager オブジェクトを作成する
func CreateManager(directoryPath string, blockSize int) *Manager {
	os.Mkdir(directoryPath, 0750)
	// err := os.Mkdir(directoryPath, 0750)
	// if err != nil && !os.IsExist(err) {
	// tmp fileは削除したい
	// defer os.RemoveAll(directoryPath);
	// }

	return &Manager{DbDirectory: directoryPath, BlockSize: blockSize, OpenFiles: map[string]*os.File{}}
}
