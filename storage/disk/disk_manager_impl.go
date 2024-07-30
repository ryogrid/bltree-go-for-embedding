// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package disk

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/ncw/directio"
	"github.com/ryogrid/bltree-go-for-embedding/common"
	"github.com/ryogrid/bltree-go-for-embedding/types"
)

// DiskManagerImpl is the disk implementation of DiskManager
type DiskManagerImpl struct {
	db          *os.File
	fileName    string
	nextPageID  types.PageID
	numWrites   uint64
	size        int64
	flush_log   bool
	numFlushes  uint64
	dbFileMutex *sync.Mutex
}

// NewDiskManagerImpl returns a DiskManager instance
func NewDiskManagerImpl(dbFilename string) DiskManager {
	//file, err := os.OpenFile(dbFilename, os.O_RDWR|os.O_CREATE, 0666)
	file, err := directio.OpenFile(dbFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln("can't open db file")
		return nil
	}

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalln("file info error")
		return nil
	}

	fileSize := fileInfo.Size()
	nPages := fileSize / common.PageSize

	nextPageID := types.PageID(0)
	if nPages > 0 {
		nextPageID = types.PageID(int32(nPages + 1))
	}

	return &DiskManagerImpl{file, dbFilename, nextPageID, 0, fileSize, false, 0, new(sync.Mutex)}
}

// ShutDown closes of the database file
func (d *DiskManagerImpl) ShutDown() {
	d.dbFileMutex.Lock()
	err := d.db.Close()
	if err != nil {
		fmt.Println(err)
		panic("close of db file failed")
	}
	d.dbFileMutex.Unlock()
}

// Write a page to the database file
func (d *DiskManagerImpl) WritePage(pageId types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageId) * int64(common.PageSize)
	_, errSeek := d.db.Seek(offset, io.SeekStart)
	if errSeek != nil {
		fmt.Println(errSeek)
		// TODO: (SDB) SHOULD BE FIXED: checkpoint thread's call causes this error rarely
		fmt.Println("WritePge: d.db.Write returns err!")
		return errSeek
	}
	block := directio.AlignedBlock(directio.BlockSize)
	copy(block, pageData)
	//bytesWritten, errWrite := d.db.Write(pageData)

	// this works because directio.BlockSize == common.PageSize
	bytesWritten, errWrite := d.db.Write(block)
	if errWrite != nil {
		fmt.Println(errWrite)
		panic("WritePge: d.db.Write returns err!")
	}

	if bytesWritten != common.PageSize {
		panic("bytes written not equals page size")
	}

	if offset >= d.size {
		d.size = offset + int64(bytesWritten)
	}

	//d.db.Sync()
	return nil
}

// Read a page from the database file
func (d *DiskManagerImpl) ReadPage(pageID types.PageID, pageData []byte) error {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	offset := int64(pageID) * int64(common.PageSize)

	fileInfo, err := d.db.Stat()
	if err != nil {
		fmt.Println(err)
		// TODO: (SDB) SHOULD BE FIXED: checkpoint and statics data update thread's call causes this error rarely
		return errors.New("file info error")
	}

	if offset > fileInfo.Size() {
		return errors.New("I/O error past end of file")
	}

	d.db.Seek(offset, io.SeekStart)

	bytesRead, err := d.db.Read(pageData)
	if err != nil {
		return errors.New("I/O error while reading")
	}

	if bytesRead < common.PageSize {
		for i := 0; i < common.PageSize; i++ {
			pageData[i] = 0
		}
	}
	return nil
}

// AllocatePage allocates a new page
func (d *DiskManagerImpl) AllocatePage() types.PageID {
	d.dbFileMutex.Lock()

	ret := d.nextPageID
	defer d.dbFileMutex.Unlock()

	d.nextPageID++
	return ret
}

// DeallocatePage deallocates page
// Need bitmap in header page for tracking pages
// This does not actually need to do anything for now.
func (d *DiskManagerImpl) DeallocatePage(pageID types.PageID) {}

// GetNumWrites returns the number of disk writes
func (d *DiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// Size returns the size of the file in disk
func (d *DiskManagerImpl) Size() int64 {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()
	return d.size
}

// ATTENTION: this method can be call after calling of Shutdown method
func (d *DiskManagerImpl) RemoveDBFile() {
	d.dbFileMutex.Lock()
	defer d.dbFileMutex.Unlock()

	err := os.Remove(d.fileName)
	if err != nil {
		fmt.Println(err)
		panic("file remove failed")
	}
}
