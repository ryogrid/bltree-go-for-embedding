package blink_tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ryogrid/sametree/lib/storage/buffer"
	shpage "github.com/ryogrid/sametree/lib/storage/page"
	"github.com/ryogrid/sametree/lib/types"
)

const HASH_TABLE_ENTRY_CHAIN_LEN = 16

type (
	PageZero struct {
		alloc []byte      // next page_no in right ptr
		dups  uint64      // global duplicate key unique id
		chain [BtId]uint8 // head of free page_nos chain
	}
	BufMgr struct {
		pageSize     uint32 // page size
		pageBits     uint8  // page size in bits
		pageDataSize uint32 // page data size

		pageZero      PageZero
		lock          SpinLatch   // allocation area lite latch
		latchDeployed uint32      // highest number of latch entries deployed
		nLatchPage    uint        // number of latch pages at BT_latch
		latchTotal    uint        // number of page latch entries
		latchHash     uint        // number of latch hash table slots (latch hash table slots の数)
		latchVictim   uint32      // next latch entry to examine
		hashTable     []HashEntry // the buffer pool hash table entries
		latchs        []Latchs    // mapped latch set from buffer pool
		pagePool      []Page      // mapped to the buffer pool pages
		bpm           *buffer.BufferPoolManager
		pageIdConvMap *sync.Map // page id conversion map: Uid -> types.PageID

		err BLTErr // last error
	}
)

func (z *PageZero) AllocRight() *[BtId]byte {
	rightStart := 4*4 + 1 + 1 + 1 + 1
	return (*[6]byte)(z.alloc[rightStart : rightStart+6])
}

func (z *PageZero) SetAllocRight(pageNo Uid) {
	PutID(z.AllocRight(), pageNo)
}

// NewBufMgr creates a new buffer manager
func NewBufMgr(name string, bits uint8, nodeMax uint, bpm *buffer.BufferPoolManager, lastPageZeroId *types.PageID) *BufMgr {
	initit := true

	// determine sanity of page size
	if bits > BtMaxBits {
		bits = BtMaxBits
	} else if bits < BtMinBits {
		bits = BtMinBits
	}

	// determine sanity of buffer pool
	if nodeMax < HASH_TABLE_ENTRY_CHAIN_LEN {
		panic(fmt.Sprintf("Buffer pool too small: %d\n", nodeMax))
	}

	mgr := BufMgr{}

	mgr.bpm = bpm
	mgr.pageIdConvMap = new(sync.Map)

	mgr.pageSize = 1 << bits
	mgr.pageBits = bits
	mgr.pageDataSize = mgr.pageSize - PageHeaderSize

	if lastPageZeroId != nil {
		var page Page

		shPageZero := mgr.bpm.FetchPage(*lastPageZeroId)
		if shPageZero == nil {
			panic("failed to fetch page")
		}

		page.Data = shPageZero.Data()[PageHeaderSize:]
		//pageZeroBytes = pageBytes
		mgr.pageZero.alloc = shPageZero.Data()[:]
		mgr.loadPageIdMapping(shPageZero)
		//mgr.loadPageIdMapping(shPageZero, false)

		if err2 := binary.Read(bytes.NewReader(mgr.pageZero.alloc), binary.LittleEndian, &page.PageHeader); err2 != nil {
			panic(fmt.Sprintf("Unable to read btree file: %v\n", err2))
		}

		initit = false
	}

	// calculate number of latch hash table entries
	// Note: in original code, calculate using HashEntry size
	// `mgr->nlatchpage = (nodemax/HASH_TABLE_ENTRY_CHAIN_LEN * sizeof(HashEntry) + mgr->page_size - 1) / mgr->page_size;`
	mgr.latchHash = nodeMax / HASH_TABLE_ENTRY_CHAIN_LEN

	mgr.latchTotal = nodeMax

	mgr.hashTable = make([]HashEntry, mgr.latchHash)
	mgr.latchs = make([]Latchs, mgr.latchTotal)
	mgr.pagePool = make([]Page, mgr.latchTotal)

	var allocBytes []byte
	if initit {
		alloc := NewPage(mgr.pageDataSize)
		alloc.Bits = mgr.pageBits
		PutID(&alloc.Right, MinLvl+1)

		if mgr.PageOut(alloc, 0, true) != BLTErrOk {
			panic("Unable to create btree page zero\n")
		}

		// store page zero data to map to BufMgr::pageZero.alloc
		buf := bytes.NewBuffer(make([]byte, 0, mgr.pageSize))
		if err2 := binary.Write(buf, binary.LittleEndian, alloc.PageHeader); err2 != nil {
			panic(fmt.Sprintf("Unable to output page header as bytes: %v\n", err2))
		}
		//mgr.pageZero.alloc = buf.Bytes()
		allocBytes = buf.Bytes()
		allocBytes = append(allocBytes, make([]byte, mgr.pageSize-PageHeaderSize)...)
		mgr.pageZero.alloc = allocBytes

		alloc = NewPage(mgr.pageDataSize)
		alloc.Bits = mgr.pageBits

		for lvl := MinLvl - 1; lvl >= 0; lvl-- {
			z := uint32(1) // size of BLTVal
			if lvl > 0 {   // only page 0
				z += BtId
			}
			alloc.SetKeyOffset(1, mgr.pageDataSize-3-z)
			// create stopper key
			alloc.SetKey([]byte{0xff, 0xff}, 1)

			if lvl > 0 {
				var value [BtId]byte
				PutID(&value, Uid(MinLvl-lvl+1))
				alloc.SetValue(value[:], 1)
			} else {
				alloc.SetValue([]byte{}, 1)
			}

			alloc.Min = alloc.KeyOffset(1)
			alloc.Lvl = uint8(lvl)
			alloc.Cnt = 1
			alloc.Act = 1

			if err3 := mgr.PageOut(alloc, Uid(MinLvl-lvl), true); err3 != BLTErrOk {
				panic("Unable to create btree page zero\n")
			}
		}
	}

	return &mgr
}

func (mgr *BufMgr) PageIn(page *Page, pageNo Uid) BLTErr {
	//fmt.Println("PageIn pageNo: ", pageNo)

	if shPageId, ok := mgr.pageIdConvMap.Load(pageNo); ok {
		shPage := mgr.bpm.FetchPage(shPageId.(types.PageID))
		if shPage == nil {
			panic("failed to fetch page")
		}
		headerBuf := bytes.NewBuffer(shPage.Data()[:PageHeaderSize])
		binary.Read(headerBuf, binary.LittleEndian, &page.PageHeader)
		page.Data = (*shPage.Data())[PageHeaderSize:]
	} else {
		panic("page mapping not found")
	}

	return BLTErrOk
}

// writePage writes a page to permanent location in BLTree file,
// and clear the dirty bit (← clear していない...)
func (mgr *BufMgr) PageOut(page *Page, pageNo Uid, isDirty bool) BLTErr {
	//fmt.Println("PageOut pageNo: ", pageNo)

	shPageId := types.PageID(-1)
	isNoEntry := false
	if val, ok := mgr.pageIdConvMap.Load(pageNo); !ok {
		isNoEntry = true
		shPageId = types.PageID(-1)
	} else {
		shPageId = val.(types.PageID)
	}

	var shPage *shpage.Page = nil

	if isNoEntry {
		// called for not existing page case

		//fmt.Println("PageOut: new page... : ", pageNo)

		// create new page on SamehadaDB's buffer pool and db file
		// 1 pin count is left
		shPage = mgr.bpm.NewPage()
		if shPage == nil {
			panic("failed to create new page")
		}
		if isDirty {
			copy(shPage.Data()[PageHeaderSize:], page.Data)
			headerBuf := bytes.NewBuffer(make([]byte, 0, PageHeaderSize))
			binary.Write(headerBuf, binary.LittleEndian, page.PageHeader)
			headerBytes := headerBuf.Bytes()
			copy(shPage.Data()[:PageHeaderSize], headerBytes)
			if _, ok := mgr.pageIdConvMap.Load(pageNo); ok {
				panic("page already exists")
			}
		}
		shPageId = shPage.GetPageId()
		mgr.pageIdConvMap.Store(pageNo, shPageId)
	}

	if shPage == nil {
		shPage = mgr.bpm.FetchPage(shPageId)
		if shPage == nil {
			panic("failed to fetch page")
		}
		// decrement pin count because the count is incremented at FetchPage
		if shPage.PinCount() == 2 {
			shPage.DecPinCount()
		}
	}

	if isDirty && !isNoEntry {
		headerBuf := bytes.NewBuffer(make([]byte, 0, PageHeaderSize))
		binary.Write(headerBuf, binary.LittleEndian, page.PageHeader)
		headerBytes := headerBuf.Bytes()
		copy(shPage.Data()[:PageHeaderSize], headerBytes)
		copy(shPage.Data()[PageHeaderSize:], page.Data)
		//fmt.Println("PageOut: write page. dirty!")
	} else {
		//fmt.Println("PageOut: write page. not dirty!")
	}
	mgr.bpm.UnpinPage(shPageId, isDirty)

	//fmt.Println("PageOut: unpin paged. pageNo:", pageNo, "shPageId:", shPageId, "pin count: ", shPage.PinCount())

	return BLTErrOk
}

// flush page 0 and dirty pool pages
// persist page id mapping info and free page IDs
func (mgr *BufMgr) Close() {
	num := 0

	// flush page 0
	pageZeroVal := Page{}
	pageZero := &pageZeroVal
	pageZero.PageHeader.Right = *mgr.pageZero.AllocRight()
	pageZero.PageHeader.Bits = mgr.pageBits
	pageZero.Data = mgr.pageZero.alloc[PageHeaderSize:]

	// flush dirty pool pages
	var slot uint32
	for slot = 1; slot <= mgr.latchDeployed; slot++ {
		page := &mgr.pagePool[slot]
		latch := &mgr.latchs[slot]

		if latch.dirty {
			mgr.PageOut(page, latch.pageNo, true)
			latch.dirty = false
			num++
		}
	}

	fmt.Println(num, "dirty pages flushed")

	// Note: bpm.FetchPage and mgr.PageOut is called in these methods call
	mgr.serializePageIdMappingToPage(pageZero)

	mgr.deleterFreePages()

	mgr.PageOut(pageZero, 0, true)
}

// deallocate free pages from SamehadaDB's buffer pool
// these page ID is not used in BLTree forever
func (mgr *BufMgr) deleterFreePages() {
	makeFreePageMap := func() *sync.Map {
		freePageMap := sync.Map{}
		var read uint
		var write uint
		set := &PageSet{}
		set.page = &Page{}
		PutID(&set.page.Right, GetID(&mgr.pageZero.chain))
		for {
			freePageNo := GetID(&set.page.Right)
			if freePageNo > 0 {
				set.latch = mgr.PinLatch(freePageNo, false, &read, &write)
				if set.latch != nil {
					set.page = mgr.GetRefOfPageAtPool(set.latch)
					if set.page.Free {
						//fmt.Println("free page found: ", freePageNo)
						freePageMap.Store(freePageNo, true)
					} else {
						break
					}
				} else {
					break
				}
			} else {
				break
			}
		}
		return &freePageMap
	}

	freePageMap := makeFreePageMap()
	freePageMap.Range(func(key, value interface{}) bool {
		pageNo := key.(Uid)
		if shPageId, ok := mgr.pageIdConvMap.Load(pageNo); ok {
			mgr.bpm.DeallocatePage(shPageId.(types.PageID), true)
			mgr.pageIdConvMap.Delete(pageNo)
		}
		//fmt.Println("deallocate free page: ", pageNo)

		return true
	})
}

func (mgr *BufMgr) serializePageIdMappingToPage(pageZero *Page) {
	// format
	// page 0: | page header (26bytes) | next samehada page Id for page Id mapping info (4bytes) | mapping count or free blink-tree page count in page (4bytes) | entry-0 (12bytes) | entry-1 (12bytes) | ... |
	// entry: | blink tree page id (int64 8bytes) | samehada page id (uint32 4bytes) |
	// NOTE: pages are chained with next samehada page id and next free blink-tree page id
	//       but chain is separated to two chains.
	//       page id mapping info is stored in page 0 and chain which uses next samehada page Id
	//       free blink-tree page info is not stored in page 0 but pointer for it is stored in page 0
	//       and the chain uses next free blink-tree page ID
	//       when next page does not exist, next xxxxx ID is set to 0xffffffff (uint32 max value and -1 as int32)

	var curPage Page
	mappingCnt := uint32(0)

	serializeIdMappingEntryFunc := func(key, value interface{}) {
		pageNo := key.(Uid)
		shPageId := value.(types.PageID)
		buf := make([]byte, PageIdMappingEntrySize)
		binary.LittleEndian.PutUint64(buf[:PageIdMappingBLETreePageSize], uint64(pageNo))
		binary.LittleEndian.PutUint32(buf[PageIdMappingBLETreePageSize:PageIdMappingBLETreePageSize+PageIdMappingShPageSize], uint32(shPageId))
		offset := (NextShPageIdForIdMappingSize + EntryCountSize) + mappingCnt*PageIdMappingEntrySize
		copy(curPage.Data[offset:offset+PageIdMappingEntrySize], buf)
	}

	maxSerializeNum := (mgr.pageDataSize - (NextShPageIdForIdMappingSize + EntryCountSize)) / PageIdMappingEntrySize

	curPage.Data = pageZero.Data
	pageId := mgr.GetMappedShPageIdOfPageZero()

	isPageZero := true

	itrFunc := func(key, value interface{}) bool {
		// write data
		serializeIdMappingEntryFunc(key, value)

		mappingCnt++
		if mappingCnt >= maxSerializeNum {
			// reached capacity limit
			// TODO: (sametree) need to reuse page if already allocated
			shPage := mgr.bpm.NewPage()
			if shPage == nil {
				panic("failed to create new page")
			}
			nextPageId := shPage.GetPageId()
			// write mapping data header
			buf2 := make([]byte, ShPageIdSize)
			binary.LittleEndian.PutUint32(buf2, uint32(nextPageId))
			copy(curPage.Data[:NextShPageIdForIdMappingSize], buf2)
			binary.LittleEndian.PutUint32(buf2, mappingCnt)
			copy(curPage.Data[NextShPageIdForIdMappingSize:NextShPageIdForIdMappingSize+EntryCountSize], buf2)

			// write back to SamehadaDB's buffer pool
			if isPageZero {
				//mgr.PageOut(curPage, Uid(0), true)
				isPageZero = false
			} else {
				// free samehada page
				// (calling PageOut is not needed due to page header is not used in this case)
				mgr.bpm.UnpinPage(pageId, true)
			}

			pageId = nextPageId
			// page header is not copied due to it is not used
			curPage.Data = shPage.Data()[PageHeaderSize:]
			mappingCnt = 0
		}
		return true
	}

	mgr.pageIdConvMap.Range(itrFunc)

	// write mapping data header
	buf := make([]byte, ShPageIdSize)
	// -1 as int32
	// this is marker for end of mapping data
	binary.LittleEndian.PutUint32(buf, uint32(0xffffffff))
	copy(curPage.Data[:NextShPageIdForIdMappingSize], buf)
	binary.LittleEndian.PutUint32(buf, mappingCnt)
	copy(curPage.Data[NextShPageIdForIdMappingSize:NextShPageIdForIdMappingSize+EntryCountSize], buf)

	// write back to SamehadaDB's buffer pool
	if !isPageZero {
		// free samehada page
		// (calling PageOut is not needed due to page header is not used in this case)
		mgr.bpm.UnpinPage(pageId, true)
	}
}

func (mgr *BufMgr) loadPageIdMapping(pageZero *shpage.Page) {
	// deserialize page mapping data from page zero
	isPageZero := true
	var curShPage *shpage.Page
	curShPage = pageZero
	for {
		offset := PageHeaderSize
		mappingCnt := binary.LittleEndian.Uint32(curShPage.Data()[offset+NextShPageIdForIdMappingSize : offset+NextShPageIdForIdMappingSize+EntryCountSize])
		offset += NextShPageIdForIdMappingSize + EntryCountSize
		for ii := 0; ii < int(mappingCnt); ii++ {
			pageNo := Uid(binary.LittleEndian.Uint64(curShPage.Data()[offset : offset+PageIdMappingBLETreePageSize]))
			offset += PageIdMappingBLETreePageSize
			shPageId := types.PageID(binary.LittleEndian.Uint32(curShPage.Data()[offset : offset+PageIdMappingShPageSize]))
			offset += PageIdMappingShPageSize
			mgr.pageIdConvMap.Store(pageNo, shPageId)
		}
		offset = PageHeaderSize

		nextShPageNo := int32(binary.LittleEndian.Uint32(curShPage.Data()[offset : offset+NextShPageIdForIdMappingSize]))
		if nextShPageNo == -1 {
			// page chain end
			if !isPageZero {
				mgr.bpm.UnpinPage(curShPage.GetPageId(), false)
			}
			return
		} else {
			nextShPage := mgr.bpm.FetchPage(types.PageID(nextShPageNo))
			if nextShPage == nil {
				panic("failed to fetch page")
			}
			if !isPageZero {
				// unpin current page
				mgr.bpm.UnpinPage(curShPage.GetPageId(), false)
				// deallocate current page for reuse
				mgr.bpm.DeallocatePage(curShPage.GetPageId(), true)
			}
			isPageZero = false
			curShPage = nextShPage
		}
	}
}

// poolAudit
func (mgr *BufMgr) PoolAudit() {
	var slot uint32
	for slot = 0; slot <= mgr.latchDeployed; slot++ {
		latch := mgr.latchs[slot]

		if (latch.readWr.rin & Mask) > 0 {
			errPrintf("latchset %d rwlocked for page %d\n", slot, latch.pageNo)
		}
		latch.readWr = BLTRWLock{}

		if (latch.access.rin & Mask) > 0 {
			errPrintf("latchset %d access locked for page %d\n", slot, latch.pageNo)
		}
		latch.access = BLTRWLock{}

		if (latch.parent.rin & Mask) > 0 {
			errPrintf("latchset %d parentlocked for page %d\n", slot, latch.pageNo)
		}
		latch.parent = BLTRWLock{}

		if (latch.pin & ^ClockBit) > 0 {
			errPrintf("latchset %d pinned for page %d\n", slot, latch.pageNo)
			latch.pin = 0
		}
	}
}

// latchLink
func (mgr *BufMgr) LatchLink(hashIdx uint, slot uint, pageNo Uid, loadIt bool, reads *uint) BLTErr {
	page := &mgr.pagePool[slot]
	latch := &mgr.latchs[slot]

	if he := &mgr.hashTable[hashIdx]; he != nil {
		latch.next = he.slot
		if he.slot > 0 {
			mgr.latchs[latch.next].prev = slot
		}
	} else {
		panic("hash table entry is nil")
	}

	mgr.hashTable[hashIdx].slot = slot
	latch.atomicID = 0
	latch.pageNo = pageNo
	latch.entry = slot
	latch.split = 0
	latch.prev = 0
	latch.pin = 1

	if loadIt {
		if mgr.err = mgr.PageIn(page, pageNo); mgr.err != BLTErrOk {
			return mgr.err
		}
		*reads++
	}

	mgr.err = BLTErrOk
	return mgr.err
}

// MapPage maps a page from the buffer pool
func (mgr *BufMgr) GetRefOfPageAtPool(latch *Latchs) *Page {
	return &mgr.pagePool[latch.entry]
}

// PinLatch pins a page in the buffer pool
func (mgr *BufMgr) PinLatch(pageNo Uid, loadIt bool, reads *uint, writes *uint) *Latchs {
	hashIdx := uint(pageNo) % mgr.latchHash

	// try to find our entry
	mgr.hashTable[hashIdx].latch.SpinWriteLock()
	defer mgr.hashTable[hashIdx].latch.SpinReleaseWrite()

	slot := mgr.hashTable[hashIdx].slot
	for slot > 0 {
		latch := &mgr.latchs[slot]
		if latch.pageNo == pageNo {
			break
		}
		slot = latch.next
	}

	// found our entry increment clock
	if slot > 0 {
		latch := &mgr.latchs[slot]
		atomic.AddUint32(&latch.pin, 1)

		return latch
	}

	// see if there are any unused pool entries

	slot = uint(atomic.AddUint32(&mgr.latchDeployed, 1))
	if slot < mgr.latchTotal {
		latch := &mgr.latchs[slot]
		if mgr.LatchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			return nil
		}

		return latch
	}

	atomic.AddUint32(&mgr.latchDeployed, DECREMENT)

	for {
		slot = uint(atomic.AddUint32(&mgr.latchVictim, 1) - 1)

		// try to get write lock on hash chain
		// skip entry if not obtained or has outstanding pins
		slot %= mgr.latchTotal

		if slot == 0 {
			continue
		}
		latch := &mgr.latchs[slot]
		idx := uint(latch.pageNo) % mgr.latchHash

		// see we are on same chain as hashIdx
		if idx == hashIdx {
			continue
		}
		if !mgr.hashTable[idx].latch.SpinWriteTry() {
			continue
		}

		// skip this slot if it is pinned or the CLOCK bit is set
		if latch.pin > 0 {
			if latch.pin&ClockBit > 0 {
				FetchAndAndUint32(&latch.pin, ^ClockBit)
			}
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			continue
		}

		//  update permanent page area in btree from buffer pool
		page := mgr.pagePool[slot]

		//if latch.dirty {
		//if err := mgr.PageOut(&page, latch.pageNo, latch.dirty); err != BLTErrOk {
		if err := mgr.PageOut(&page, latch.pageNo, latch.dirty); err != BLTErrOk {
			return nil
		} else {
			//for relase SamehadaDB page's memory
			page.Data = nil

			latch.dirty = false
			*writes++
		}
		//}

		//  unlink our available slot from its hash chain
		if latch.prev > 0 {
			mgr.latchs[latch.prev].next = latch.next
		} else {
			mgr.hashTable[idx].slot = latch.next
		}

		if latch.next > 0 {
			mgr.latchs[latch.next].prev = latch.prev
		}

		if mgr.LatchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			return nil
		}
		mgr.hashTable[idx].latch.SpinReleaseWrite()

		return latch
	}
}

// UnpinLatch unpins a page in the buffer pool
func (mgr *BufMgr) UnpinLatch(latch *Latchs) {
	if ^latch.pin&ClockBit > 0 {
		FetchAndOrUint32(&latch.pin, ClockBit)
	}
	atomic.AddUint32(&latch.pin, DECREMENT)
}

// NewPage allocate a new page
// returns the page with latched but unlocked
// Uid argument is used only for BufMgr initialization
func (mgr *BufMgr) NewPage(set *PageSet, contents *Page, reads *uint, writes *uint) BLTErr {
	// lock allocation page
	mgr.lock.SpinWriteLock()

	//fmt.Println("NewPage(1):  pageNo: ", GetID(&mgr.pageZero.chain))

	// use empty chain first, else allocate empty page
	pageNo := GetID(&mgr.pageZero.chain)
	if pageNo > 0 {
		//fmt.Println("NewPage(2):  pageNo: ", pageNo)
		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		if set.latch != nil {
			set.page = mgr.GetRefOfPageAtPool(set.latch)
		} else {
			mgr.err = BLTErrStruct
			return mgr.err
		}

		PutID(&mgr.pageZero.chain, GetID(&set.page.Right))
		mgr.lock.SpinReleaseWrite()
		MemCpyPage(set.page, contents)

		set.latch.dirty = true
		mgr.err = BLTErrOk
		return mgr.err
	}

	pageNo = GetID(mgr.pageZero.AllocRight())
	mgr.pageZero.SetAllocRight(pageNo + 1)

	// unlock allocation latch
	mgr.lock.SpinReleaseWrite()

	// don't load cache from btree page
	set.latch = mgr.PinLatch(pageNo, false, reads, writes)
	if set.latch != nil {
		//fmt.Println("NewPage: at mgr.GetRefOfPageAtPool. pageNo: ", pageNo, " latch.pageNo: ", set.latch.pageNo, " latch.entry: ", set.latch.entry)
		set.page = mgr.GetRefOfPageAtPool(set.latch)
	} else {
		mgr.err = BLTErrStruct
		return mgr.err
	}

	set.page.Data = make([]byte, mgr.pageDataSize)
	MemCpyPage(set.page, contents)
	set.latch.dirty = true
	mgr.err = BLTErrOk

	return mgr.err
}

// LoadPage find and load page at given level for given key leave page read or write locked as requested
func (mgr *BufMgr) PageFetch(set *PageSet, key []byte, lvl uint8, lock BLTLockMode, reads *uint, writes *uint) uint32 {
	pageNo := RootPage
	prevPage := Uid(0)
	drill := uint8(0xff)
	var slot uint32
	var prevLatch *Latchs

	mode := LockNone
	prevMode := LockNone

	// start at root of btree and drill down
	for pageNo > 0 {
		// determine lock mode of drill level
		if drill == lvl {
			mode = lock
		} else {
			mode = LockRead
		}

		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		if set.latch == nil {
			return 0
		}

		// obtain access lock using lock chaining with Access mode
		if pageNo > RootPage {
			mgr.PageLock(LockAccess, set.latch)
		}

		set.page = mgr.GetRefOfPageAtPool(set.latch)

		// release & unpin parent page
		if prevPage > 0 {
			mgr.PageUnlock(prevMode, prevLatch)
			mgr.UnpinLatch(prevLatch)
			prevPage = Uid(0)
		}

		// skip Atomic lock on leaf page if already held
		// Note: not supported in this golang implementation
		//if (!drill) {
		//	if (mode & LockAtomic) {
		//		if (pthread_equal( set->latch->atomictid, pthread_self() )) {
		//			mode &= ~LockAtomic;
		//		}
		//	}
		//}

		// obtain mode lock using lock chaining through AccessLock
		mgr.PageLock(mode, set.latch)

		// Note: not supported in this golang implementation
		//if (mode & LockAtomic) {
		//	set->latch->atomictid = pthread_self();
		//}

		if set.page.Free {
			mgr.err = BLTErrStruct
			return 0
		}

		if pageNo > RootPage {
			mgr.PageUnlock(LockAccess, set.latch)
		}

		// re-read and re-lock root after determining actual level of root
		if set.page.Lvl != drill {
			if set.latch.pageNo != RootPage {
				mgr.err = BLTErrStruct
				return 0
			}

			drill = set.page.Lvl

			if lock != LockRead && drill == lvl {
				mgr.PageUnlock(mode, set.latch)
				mgr.UnpinLatch(set.latch)
				continue
			}
		}

		prevPage = set.latch.pageNo
		prevLatch = set.latch
		prevMode = mode

		//  find key on page at this level
		//  and descend to requested level
		if set.page.Kill {
			goto sliderRight
		}

		slot = set.page.FindSlot(key)
		if slot > 0 {
			if drill == lvl {
				return slot
			}

			for set.page.Dead(slot) {
				if slot < set.page.Cnt {
					slot++
					continue
				} else {
					goto sliderRight
				}
			}

			pageNo = GetIDFromValue(set.page.Value(slot))
			//fmt.Println("PageFetch: move from ", set.latch.pageNo, "(", set.page.Lvl, ") to ", pageNo, "(", drill-1, ")")
			drill--
			continue
		}

	sliderRight: // slide right into next page
		pageNo = GetID(&set.page.Right)
	}

	// return error on end of right chain
	mgr.err = BLTErrStruct
	return 0
}

// FreePage
//
// return page to free list
// page must be delete and write locked
func (mgr *BufMgr) PageFree(set *PageSet) {

	// lock allocation page
	mgr.lock.SpinWriteLock()

	// store chain
	set.page.Right = mgr.pageZero.chain
	PutID(&mgr.pageZero.chain, set.latch.pageNo)
	set.latch.dirty = true
	set.page.Free = true

	// unlock released page
	mgr.PageUnlock(LockDelete, set.latch)
	mgr.PageUnlock(LockWrite, set.latch)
	mgr.UnpinLatch(set.latch)

	// unlock allocation page
	mgr.lock.SpinReleaseWrite()
}

// LockPage
//
// place write, read, or parent lock on requested page_no
func (mgr *BufMgr) PageLock(mode BLTLockMode, latch *Latchs) {
	switch mode {
	case LockRead:
		latch.readWr.ReadLock()
	case LockWrite:
		latch.readWr.WriteLock()
	case LockAccess:
		latch.access.ReadLock()
	case LockDelete:
		latch.access.WriteLock()
	case LockParent:
		latch.parent.WriteLock()
		//case LockAtomic: // Note: not supported in this golang implementation
	}
}

func (mgr *BufMgr) PageUnlock(mode BLTLockMode, latch *Latchs) {
	switch mode {
	case LockRead:
		latch.readWr.ReadRelease()
	case LockWrite:
		latch.readWr.WriteRelease()
	case LockAccess:
		latch.access.ReadRelease()
	case LockDelete:
		latch.access.WriteRelease()
	case LockParent:
		latch.parent.WriteRelease()
		//case LockAtomic: // Note: not supported in this golang implementation
	}
}

func (mgr *BufMgr) GetPageDataSize() uint32 {
	return mgr.pageDataSize
}
func (mgr *BufMgr) GetLatchSets() []Latchs {
	return mgr.latchs
}
func (mgr *BufMgr) GetPageBits() uint8 {
	return mgr.pageBits
}
func (mgr *BufMgr) GetPageZero() *PageZero {
	return &mgr.pageZero
}
func (mgr *BufMgr) GetPagePool() []Page {
	return mgr.pagePool
}

func (mgr *BufMgr) GetMappedShPageIdOfPageZero() types.PageID {
	if val, ok := mgr.pageIdConvMap.Load(Uid(0)); ok {
		ret := val.(types.PageID)
		return ret
	} else {
		panic("page zero mapping not found")
	}
}
