package page

type ParentPageImple struct {
	*Page
}

func (p *ParentPageImple) DecPPinCount() {
	p.DecPinCount()
}

func (p *ParentPageImple) PinCount() int32 {
	return p.PinCount()
}

func (p *ParentPageImple) GetPageId() int32 {
	return p.GetPageId()
}

func (p *ParentPageImple) DataAsSlice() []byte {
	return p.DataAsSlice()
}
