package metadatas

import (
	"github.com/hirokihello/hhdb/src/records"
	"github.com/hirokihello/hhdb/src/transactions"
)

type StatManager struct {
	tableManager *TableManger
	tableStats   map[string]StatInfo
	numCalls     int
}

type StatInfo struct {
	numBlocks  int
	numRecords int
}

func CreateStatManager(
	tableManager *TableManger,
	transaction *transactions.Transaction,
) *StatManager {
	statManager := StatManager{
		tableManager: tableManager,
		tableStats:   make(map[string]StatInfo),
		numCalls:     0,
	}
	statManager.RefreshStatistics(transaction)
	return &statManager
}

func (s *StatManager) GetStatInfo(
	tableName string,
	layout *records.Layout,
	transaction *transactions.Transaction) StatInfo {
	s.numCalls++

	if s.numCalls > 100 {
		s.RefreshStatistics(transaction)
	}

	statInfo := s.tableStats[tableName]
	// これでいいのかは自信がない
	if statInfo == (StatInfo{}) {
		statInfo = calcTableStats(tableName, layout, transaction)
		s.tableStats[tableName] = statInfo
	}

	return statInfo
}

func (s *StatManager) RefreshStatistics(
	transaction *transactions.Transaction,
) {
	s.tableStats = make(map[string]StatInfo)
	s.numCalls = 0
	layout := s.tableManager.GetLayout(TABLE_CATALOG, transaction)
	tableScan := records.CreateTableScan(transaction, TABLE_CATALOG, layout)

	for tableScan.Next() {
		tableName := tableScan.GetString(TABLE_NAME)
		layout := s.tableManager.GetLayout(tableName, transaction)
		statInfo := calcTableStats(tableName, layout, transaction)
		s.tableStats[tableName] = statInfo
	}
	tableScan.Close()
}

func calcTableStats(
	tableName string,
	layout *records.Layout,
	transaction *transactions.Transaction,
) StatInfo {
	numRecords := 0
	numBlocks := 0
	tableScan := records.CreateTableScan(transaction, tableName, layout)
	for tableScan.Next() {
		numRecords++
		numBlocks = tableScan.GetRid().BlockNumber() + 1
	}
	tableScan.Close()
	return CreateStatInfo(numBlocks, numRecords)
}

func CreateStatInfo(numBlocks int, numRecords int) StatInfo {
	return StatInfo{
		numBlocks:  numBlocks,
		numRecords: numRecords,
	}
}

func (s *StatInfo) blockAccessed() int {
	return s.numBlocks
}

func (s *StatInfo) recordsOutput() int {
	return s.numRecords
}

func (s *StatInfo) distinctValues(fieldName string) int {
	return 1 + (s.numRecords / 3)
}
