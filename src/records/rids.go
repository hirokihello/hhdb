package records

import "strconv"

type Rid struct {
	slot        int
	blockNumber int
}

func createRid(slot int, blockNumber int) *Rid {
	return &Rid{slot: slot, blockNumber: blockNumber}
}

func (rid *Rid) BlockNumber() int {
	return rid.blockNumber
}

func (rid *Rid) Slot() int {
	return rid.slot
}

func (rid *Rid) Equal(other *Rid) bool {
	return other.blockNumber == rid.blockNumber && other.slot == rid.slot
}

func (rid *Rid) ToString() string {
	return "[" + strconv.Itoa(rid.slot) + ", " + strconv.Itoa(rid.blockNumber) + "]"
}
