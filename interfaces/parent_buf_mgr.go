package interfaces

type ParentBufMgr interface {
	FetchPPage(pageID int32) ParentPage
	UnpinPPage(pageID int32, isDirty bool) error
	NewPPage() ParentPage
	DeallocatePPage(pageID int32, isNoWait bool) error
}
