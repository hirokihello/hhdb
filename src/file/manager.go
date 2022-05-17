package dbFile;

import (
	"os"
	"fmt"
)

type Manager struct {
	DbDirectory string;
	BlockSize int;
	OpenFiles map[string] *os.File
}

type ManagerI interface {
	Write();
	Read();
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
		a.OpenFiles[fileName] = file;
		f = file;
	}

	return f;
}

func (a *Manager) Read (blk Block, page Page) {
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
}

// writeはpageの内容をfileに書き込む
func (a *Manager) Write (blk Block, page Page) {
	file := a.GetFile(blk.FileName);
	// 第二引数0はファイルの先頭からのoffsetを示す
	file.Seek(int64(blk.BlockNumber * a.BlockSize), 0);
	file.Write(page.Contents());
}

func CreateManager (directoryPath string, blockSize int) Manager {
	err := os.Mkdir(directoryPath, 0750)
	if err != nil && !os.IsExist(err) {
		// tmp fileは削除したい
		// defer os.RemoveAll(directoryPath);
	}

	return Manager{DbDirectory: directoryPath, BlockSize: blockSize, OpenFiles: map[string] *os.File{}};
}
