package recoveries

import (
	"github.com/hirokihello/hhdb/src/buffers"
	"github.com/hirokihello/hhdb/src/logs"
)

type Manager struct {
	logManager    logs.Manager
	bufferManager buffers.Manager
	tx            transactions.Transaction
	txnum         int
}
