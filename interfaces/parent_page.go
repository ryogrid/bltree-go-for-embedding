package interfaces

type ParentPage interface {
	DecPinCount()
	PinCount() int32
	GetPageId() int32
	DataAsSlice() []byte
}
