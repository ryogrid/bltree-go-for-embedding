package interfaces

type ParentPage interface {
	DecPPinCount()
	PPinCount() int32
	GetPPageId() int32
	DataAsSlice() []byte
}
