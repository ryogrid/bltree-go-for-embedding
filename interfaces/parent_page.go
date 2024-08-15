package interfaces

type ParentPage interface {
	DecPPinCount()
	PinCount() int32
	GetPageId() int32
	DataAsSlice() []byte
}
