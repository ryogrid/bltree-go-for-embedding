package interfaces

type ParentBufMgr interface {
	FetchPage(pageID int32) ParentPage
	UnpinPage(pageID int32, isDirty bool) error
	NewPage() ParentPage
	DeallocatePage(pageID int32, isNoWait bool) error
}
