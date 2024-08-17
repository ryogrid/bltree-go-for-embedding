package blink_tree

import (
	"github.com/ryogrid/bltree-go-for-embedding/interfaces"
	"sync"
	"sync/atomic"
)

// for ParentBufMgrDummy
var nectPageID int32 = 0

// this class is ParentBufMgr interface implementation sample
// store data in memory only and don't manage memory usage
type ParentBufMgrDummy struct {
	pageMap *sync.Map // key: pageID, value: ParentPage
}

func NewParentBufMgrDummy(baseMap *sync.Map) interfaces.ParentBufMgr {
	if baseMap != nil {
		// when BufMgr is reconstructed, use the given map
		return &ParentBufMgrDummy{pageMap: baseMap}
	} else {
		// when BufMgr is newly created, create new map
		return &ParentBufMgrDummy{pageMap: &sync.Map{}}
	}
}

func (p *ParentBufMgrDummy) FetchPPage(pageID int32) interfaces.ParentPage {
	if val, ok := p.pageMap.Load(pageID); ok {
		ret := val.(interfaces.ParentPage)
		tmp := ret.(*ParentPageDummy)
		// increment pin count
		atomic.AddInt32(&tmp.pincCount, 1)
		return ret
	} else {
		panic("unknown pageID")
	}
}

func (p *ParentBufMgrDummy) UnpinPPage(pageID int32, isDirty bool) error {
	if val, ok := p.pageMap.Load(pageID); ok {
		ppage := val.(interfaces.ParentPage)
		ppage.DecPPinCount()
		return nil
	} else {
		panic("unknown pageID")
	}
}

func (p *ParentBufMgrDummy) NewPPage() interfaces.ParentPage {
	newPageID := atomic.AddInt32(&nectPageID, 1)
	newPage := NewParentPageDummy(newPageID, 1, [4096]byte{})
	p.pageMap.Store(newPageID, newPage)
	return newPage
}

func (p *ParentBufMgrDummy) DeallocatePPage(pageID int32, _isNoWait bool) error {
	if _, ok := p.pageMap.Load(pageID); ok {
		p.pageMap.Delete(pageID)
		return nil
	} else {
		panic("unknown pageID")
	}
}
