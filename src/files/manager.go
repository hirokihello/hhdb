package files

import (
	"fmt"
	"os"
	"sync"
)

type Manager struct {
	DbDirectory string // create されるときに引数で渡される
	BlockSize   int    // 1 block の byte 数
	OpenFiles   map[string]*os.File
	mu          sync.Mutex // guards
}

type ManagerI interface {
	Write()
	Read()
	Int()
	Append()
}

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

// 物理的なfileの内容を Page に書き込んでメモリ上で保持する
// ページサイズの分だけ読み込む
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

// writeはpageの内容を物理的なfileに書き込む
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

// ファイルのブロックの数を返す
func (manager *Manager) FileBlockLength(fileName string) int {
	file := manager.GetFile(fileName)
	info, _ := file.Stat()

	return int(int(info.Size()) / manager.BlockSize)
}

// Appendは既存のファイルの最終block後ろにBlockSize分の領域を確保して、そこに割り当てたblockIdとfilenameを持つBlockを返してくれる
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

func CreateManager(directoryPath string, blockSize int) *Manager {
	os.Mkdir(directoryPath, 0750)
	// err := os.Mkdir(directoryPath, 0750)
	// if err != nil && !os.IsExist(err) {
	// tmp fileは削除したい
	// defer os.RemoveAll(directoryPath);
	// }

	return &Manager{DbDirectory: directoryPath, BlockSize: blockSize, OpenFiles: map[string]*os.File{}}
}
