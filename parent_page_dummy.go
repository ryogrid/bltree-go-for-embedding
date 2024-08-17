package blink_tree

import (
	"github.com/ryogrid/bltree-go-for-embedding/interfaces"
	"sync/atomic"
)

// this class is ParentPage interface implementation sample
type ParentPageDummy struct {
	pageId    int32
	pincCount int32
	data      [4096]byte // 4KB (2^12 => 4096)
}

func NewParentPageDummy(pageId int32, initialPincCnt int32, baseData [4096]byte) interfaces.ParentPage {
	return &ParentPageDummy{pageId, initialPincCnt, baseData}
}

func (ppd *ParentPageDummy) DecPPinCount() {
	atomic.AddInt32(&ppd.pincCount, -1)
}

func (ppd *ParentPageDummy) PPinCount() int32 {
	return ppd.pincCount
}

func (ppd *ParentPageDummy) GetPPageId() int32 {
	return ppd.pageId
}

func (ppd *ParentPageDummy) DataAsSlice() []byte {
	return ppd.data[:]
}
