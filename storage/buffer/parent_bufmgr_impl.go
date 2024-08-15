package buffer

import (
	"github.com/ryogrid/bltree-go-for-embedding/interfaces"
	"github.com/ryogrid/bltree-go-for-embedding/storage/page"
	"github.com/ryogrid/bltree-go-for-embedding/types"
)

type ParentBufMgrImpl struct {
	*BufferPoolManager
}

func NewParentBufMgrImpl(bpm *BufferPoolManager) interfaces.ParentBufMgr {
	return &ParentBufMgrImpl{bpm}
}

func (p *ParentBufMgrImpl) FetchPPage(pageID int32) interfaces.ParentPage {
	return &page.ParentPageImpl{p.FetchPage(types.PageID(pageID))}
}

func (p *ParentBufMgrImpl) UnpinPPage(pageID int32, isDirty bool) error {
	return p.UnpinPage(types.PageID(pageID), isDirty)
}

func (p *ParentBufMgrImpl) NewPPage() interfaces.ParentPage {
	return &page.ParentPageImpl{p.NewPage()}
}

func (p *ParentBufMgrImpl) DeallocatePPage(pageID int32, isNoWait bool) error {
	return p.DeallocatePage(types.PageID(pageID), isNoWait)
}
