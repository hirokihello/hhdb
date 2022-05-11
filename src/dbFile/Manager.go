package dbFile;

import ("os")

type Manager struct {
	dbDirectory string;
}

func Read (a *Block) (*os.File, error) {
	file, err := os.Open(a.FileName);
	if(err != nil){
		return nil, err;
	}

	return file, nil;
}
