package interfaces

type ParentPage interface {
	DecPinCount()
	PinCount() int32
	GetPageId() int32
	Data() interface{} // *[PageSize]byte
}
