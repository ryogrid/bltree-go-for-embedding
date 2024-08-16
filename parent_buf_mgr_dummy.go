package blink_tree

import "github.com/ryogrid/bltree-go-for-embedding/interfaces"

type ParentBufMgrDummy struct {
}

func NewParentBufMgrDummy() interfaces.ParentBufMgr {
	return &ParentBufMgrDummy{}
}

func (p *ParentBufMgrDummy) FetchPPage(pageID int32) interfaces.ParentPage {
	panic("implement me")
}

func (p *ParentBufMgrDummy) UnpinPPage(pageID int32, isDirty bool) error {
	panic("implement me")
}

func (p *ParentBufMgrDummy) NewPPage() interfaces.ParentPage {
	panic("implement me")
}

func (p *ParentBufMgrDummy) DeallocatePPage(pageID int32, isNoWait bool) error {
	panic("implement me")
}
