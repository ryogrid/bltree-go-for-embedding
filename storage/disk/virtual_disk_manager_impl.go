package disk

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dsnet/golib/memfile"
	"github.com/ryogrid/blink-tree-go/common"
	"github.com/ryogrid/blink-tree-go/types"
)

// DiskManagerImpl is the disk implementation of DiskManager
type VirtualDiskManagerImpl struct {
	db          *memfile.File //[]byte
	fileName    string
	nextPageID  types.PageID
	numWrites   uint64
	size        int64
	flush_log   bool
	numFlushes  uint64
	dbFileMutex *sync.Mutex
}

func NewVirtualDiskManagerImpl(dbFilename string) DiskManager {
	file := memfile.New(make([]byte, 0))

	fileSize := int64(0)
	nextPageID := types.PageID(0)

	return &VirtualDiskManagerImpl{file, dbFilename, nextPageID, 0, fileSize, false, 0, new(sync.Mutex)}
}

// ShutDown closes of the database file
func (d *VirtualDiskManagerImpl) ShutDown() {
	// do nothing
}

// Write a page to the database file
func (d *VirtualDiskManagerImpl) WritePage(pageId types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageId) * int64(common.PageSize)
	d.db.WriteAt(pageData, offset)

	if offset >= d.size {
		d.size = offset + int64(len(pageData))
	}

	return nil
}

// Read a page from the database file
func (d *VirtualDiskManagerImpl) ReadPage(pageID types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageID) * int64(common.PageSize)

	if offset > d.size || offset+int64(len(pageData)) > d.size {
		return errors.New("I/O error past end of file")
	}

	_, err := d.db.ReadAt(pageData, offset)
	if err != nil {
		fmt.Println(err)
		panic("file read error!")
	}
	return err
}

// AllocatePage allocates a new page
func (d *VirtualDiskManagerImpl) AllocatePage() types.PageID {
	d.dbFileMutex.Lock()

	var ret types.PageID
	ret = d.nextPageID
	d.nextPageID++

	defer d.dbFileMutex.Unlock()

	return ret
}

// DeallocatePage deallocates page
func (d *VirtualDiskManagerImpl) DeallocatePage(pageID types.PageID) {
}

// GetNumWrites returns the number of disk writes
func (d *VirtualDiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// Size returns the size of the file in disk
func (d *VirtualDiskManagerImpl) Size() int64 {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()
	return d.size
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *VirtualDiskManagerImpl) RemoveDBFile() {
	// do nothing
}
