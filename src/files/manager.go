package files;

import (
	"os"
	"fmt"
	"sync"
)

type Manager struct {
	DbDirectory string;
	BlockSize int;
	OpenFiles map[string] *os.File
	mu sync.Mutex // guards
}

type ManagerI interface {
	Write();
	Read();
	Int();
	Append();
}

func (a *Manager) GetFile (fileName string) *os.File {
	// ここで使ってるファイルは基本的にblkやpageの単位のファイルと異なる....!!
	// blkやpageがどのように作られるのかはまた別の話
	f, err := a.OpenFiles[fileName];
	if(!err) {
		file, err2 := os.OpenFile(a.DbDirectory +"/"+ fileName, os.O_SYNC|os.O_RDWR, 0755);
		if err2 != nil {
			newFile, _ := os.Create(a.DbDirectory +"/" + fileName);
			file = newFile;
		}

		file.Write(make([]byte, a.BlockSize));
		file.Seek(0, 0);
		file.Sync();
		a.OpenFiles[fileName] = file;
		f = file;
	}

	return f;
}

// 物理的なfileの内容をページに書き込む
func (a *Manager) Read (blk Block, page Page) {
	a.mu.Lock();
	file := a.GetFile(blk.FileName);
	info, _ := file.Stat();
	if(info.Size() < int64((blk.BlockNumber + 1) * a.BlockSize)) {
		file.Truncate(int64((blk.BlockNumber + 1) * a.BlockSize));
	}
	n, err := file.Seek(int64(blk.BlockNumber * a.BlockSize), 0); if(err != nil) {
		fmt.Println(n);
		fmt.Println("when file.Seek(int64(blk.BlockNumber * a.BlockSize), 0) was occured, error generated: ");
	}
	read_n, err := file.Read(page.Contents()); if(err != nil) {
		fmt.Println(read_n);
		fmt.Println("file.Read(page.Contents());was occured, error occured: ");
	}
	file.Sync();
	a.mu.Unlock();
}

// writeはpageの内容を物理的なfileに書き込む
func (a *Manager) Write (blk Block, page Page) {
	a.mu.Lock();
	file := a.GetFile(blk.FileName);
	// 第二引数0はファイルの先頭からのoffsetを示す
	file.Seek(int64(blk.BlockNumber * a.BlockSize), 0);
	file.Write(page.Contents());
	file.Sync();
	a.mu.Unlock();
}

func (a *Manager) Length (fileName string) int {
	file := a.GetFile(fileName);
	info, _ := file.Stat();

	if(int(info.Size()) < a.BlockSize) {
		return 0;
	}

	return int(int(info.Size())/ a.BlockSize);
}

// Appendは既存のファイルの最終block後ろにBlockSize分の領域を確保して、そこに割り当てたblockIdとfilenameを持つBlockを返してくれる
func (mgr *Manager) Append (fileName string) *Block {
	mgr.mu.Lock();
	// ここのlengthはfileNameのサイズじゃない。ファイルに含まれるブロックの数を返す。
	blockNumber := mgr.Length(fileName);
	block := Block{FileName: fileName, BlockNumber: blockNumber-1};
	file := mgr.GetFile(fileName);
	file.Truncate(int64((1 + blockNumber) * mgr.BlockSize));

	file.Sync();
	mgr.mu.Unlock();
	return &block;
}

func CreateManager (directoryPath string, blockSize int) *Manager {
	os.Mkdir(directoryPath, 0750)
	// err := os.Mkdir(directoryPath, 0750)
	// if err != nil && !os.IsExist(err) {
		// tmp fileは削除したい
		// defer os.RemoveAll(directoryPath);
	// }

	return &Manager{DbDirectory: directoryPath, BlockSize: blockSize, OpenFiles: map[string] *os.File{}};
}
