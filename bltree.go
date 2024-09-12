package blink_tree

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

type BLTreeItr struct {
	keys   [][]byte
	vals   [][]byte
	curIdx uint32
	elems  uint32
}

func (itr *BLTreeItr) Next() (ok bool, key []byte, value []byte) {
	if itr.curIdx >= itr.elems {
		return false, nil, nil
	}
	key = itr.keys[itr.curIdx]
	value = itr.vals[itr.curIdx]
	itr.curIdx++
	return true, key, value
}

type BLTree struct {
	mgr    *BufMgr // buffer manager for thread
	cursor *Page   // cached frame for start/next (never mapped)
	// note: not use singleton frame to avoid race condition
	// frame      *Page          // spare frame for the page split (never mapped)
	cursorPage Uid // current cursor page number
	//found      bool   // last delete or insert was found (Note: not used)
	err BLTErr //last error
	//key        [KeyArray]byte // last found complete key (Note: not used)
	reads  uint // number of reads from the btree
	writes uint // number of writes to the btree
}

/*
 *  Notes:
 *
 *  Pages are allocated from low and high ends (addresses).  Key offsets
 *  and row-id's are allocated from low addresses, while the text of the key
 *  is allocated from high addresses.  When the two areas meet, the page is
 *  split with a 50% rule.  This can easily be tuned.
 *
 *  A key consists of a length byte, two bytes of index number (0 - 65534),
 *  and up to 253 bytes of key value.  Duplicate keys are discarded.
 *  Associated with each key is an opaque value of any size small enough
 *  to fit in a page.
 *
 *  The b-tree root is always located at page 1.  The first leaf page of
 *  level zero is always located on page 2.
 *
 *  The b-tree pages are linked with next pointers to facilitate
 *  enumerators and to provide for concurrency.
 *
 *  When the root page fills, it is split in two and the tree height is
 *  raised by a new root at page one with two keys.
 *
 *  Deleted keys are marked with a dead bit until page cleanup. The fence
 *  key for a node is always present
 *
 *  Groups of pages called segments from the btree are optionally cached
 *  with a memory mapped pool. A hash table is used to keep track of the
 *  cached segments.  This behavior is controlled by the cache block
 *  size parameter to open.
 *
 *  To achieve maximum concurrency one page is locked at a time as the
 *  tree is traversed to find leaf key in question. The right page numbers
 *  are used in cases where the page is being split or consolidated.
 *
 *  Page 0 is dedicated to lock for new page extensions, and chains empty
 *  pages together for reuse.
 *
 *  The ParentModification lock on a node is obtained to serialize posting
 *  or changing the fence key for a node.
 *
 *  Empty pages are chained together through the ALLOC page and reused.
 *
 *  Access macros to address slot and key values from the page Page slots
 *  use 1 based indexing.
 */

// NewBLTree open BTree access method based on buffer manager
func NewBLTree(bufMgr *BufMgr) *BLTree {
	tree := BLTree{
		mgr: bufMgr,
	}
	tree.cursor = NewPage(bufMgr.pageDataSize)

	return &tree
}

// fixFence
// a fence key was deleted from a page,
// push new fence value upwards
func (tree *BLTree) fixFence(set *PageSet, lvl uint8) BLTErr {
	// remove the old fence value
	rightKey := set.page.Key(set.page.Cnt)
	set.page.ClearSlot(set.page.Cnt)
	set.page.Cnt--
	set.latch.dirty = true

	// cache new fence value
	leftKey := set.page.Key(set.page.Cnt)

	var value [BtId]byte
	PutID(&value, set.latch.pageNo)

	if !ValidatePage(set.page) {
		panic("fixFence: page is broken.")
	}

	if !ValidatePage(set.page) {
		panic("fixFence: page is broken.")
	}

	tree.mgr.PageLock(LockParent, set.latch)
	tree.mgr.PageUnlock(LockWrite, set.latch)

	// insert new (now smaller) fence key

	if err := tree.InsertKey(leftKey, lvl+1, value, true); err != BLTErrOk {
		return err
	}

	// now delete old fence key
	if err := tree.DeleteKey(rightKey, lvl+1); err != BLTErrOk {
		return err
	}

	if !ValidatePage(set.page) {
		panic("fixFence: page is broken.")
	}

	tree.mgr.PageUnlock(LockParent, set.latch)
	tree.mgr.UnpinLatch(set.latch)

	return BLTErrOk
}

// collapseRoot
// root has a single child
// collapse a level from the tree
func (tree *BLTree) collapseRoot(root *PageSet) BLTErr {
	var child PageSet
	var pageNo Uid
	var idx uint32
	// find the child entry and promote as new root contents
	for {
		idx = 1
		for idx <= root.page.Cnt {
			if !root.page.Dead(idx) {
				break
			}
			idx++
		}

		pageNo = GetIDFromValue(root.page.Value(idx))
		child.latch = tree.mgr.PinLatch(pageNo, true, &tree.reads, &tree.writes)
		if child.latch != nil {
			child.page = tree.mgr.GetRefOfPageAtPool(child.latch)
		} else {
			return tree.err
		}

		tree.mgr.PageLock(LockDelete, child.latch)
		tree.mgr.PageLock(LockWrite, child.latch)

		if !ValidatePage(child.page) {
			panic("collapseRoot: page is broken")
		}
		MemCpyPage(root.page, child.page)
		root.latch.dirty = true
		tree.mgr.PageFree(&child)

		if !(root.page.Lvl > 1 && root.page.Act == 1) {
			break
		}
	}

	if !ValidatePage(root.page) {
		fmt.Println("collapseRoot: page is broken.")
	}
	tree.mgr.PageUnlock(LockWrite, root.latch)
	tree.mgr.UnpinLatch(root.latch)
	return BLTErrOk
}

// deletePage
//
// delete a page and manage keys
// call with page writelocked
// returns with page unpinned
func (tree *BLTree) deletePage(set *PageSet, mode BLTLockMode) BLTErr {
	var right PageSet
	// cache copy of fence key to post in parent
	lowerFence := set.page.Key(set.page.Cnt)

	// obtain lock on right page
	pageNo := GetID(&set.page.Right)
	right.latch = tree.mgr.PinLatch(pageNo, true, &tree.reads, &tree.writes)
	if right.latch != nil {
		right.page = tree.mgr.GetRefOfPageAtPool(right.latch)
	} else {
		return BLTErrOk
	}

	tree.mgr.PageLock(LockWrite, right.latch)
	tree.mgr.PageLock(mode, right.latch)

	// cache copy of key to update
	higherFence := right.page.Key(right.page.Cnt)

	if right.page.Kill {
		tree.err = BLTErrStruct
		return tree.err
	}

	// pull contents of right peer into our empty page
	MemCpyPage(set.page, right.page)
	set.latch.dirty = true

	if !ValidatePage(set.page) {
		panic("deletePage: page is broken.")
	}

	// mark right page deleted and point it to left page
	// until we can post parent updates that remove access
	// to the deleted page.
	PutID(&right.page.Right, set.latch.pageNo)
	right.latch.dirty = true
	right.page.Kill = true

	// redirect higher key directly to our new node contents
	var value [BtId]byte
	PutID(&value, set.latch.pageNo)

	tree.mgr.PageLock(LockParent, right.latch)
	tree.mgr.PageUnlock(LockWrite, right.latch)
	tree.mgr.PageUnlock(mode, right.latch)
	tree.mgr.PageLock(LockParent, set.latch)
	tree.mgr.PageUnlock(LockWrite, set.latch)

	if err := tree.InsertKey(higherFence, set.page.Lvl+1, value, true); err != BLTErrOk {
		return err
	}

	// delete old lower key to our node
	if err := tree.DeleteKey(lowerFence, set.page.Lvl+1); err != BLTErrOk {
		return err
	}

	if !ValidatePage(right.page) {
		panic("fixFence: page is broken.")
	}
	if !ValidatePage(set.page) {
		panic("fixFence: page is broken.")
	}

	// obtain delete and write locks to right node
	tree.mgr.PageUnlock(LockParent, right.latch)
	tree.mgr.PageLock(LockDelete, right.latch)
	tree.mgr.PageLock(LockWrite, right.latch)
	tree.mgr.PageFree(&right)
	tree.mgr.PageUnlock(LockParent, set.latch)
	tree.mgr.UnpinLatch(set.latch)
	//tree.found = true
	return BLTErrOk
}

// DeleteKey
//
// find and delete key on page by marking delete flag bit
// if page becomes empty, delete it from the btree
func (tree *BLTree) DeleteKey(key []byte, lvl uint8) BLTErr {
	var set PageSet

	slot := tree.mgr.PageFetch(&set, key, lvl, LockWrite, &tree.reads, &tree.writes)
	if slot == 0 {
		return tree.err
	}
	ptr := set.page.Key(slot)

	if !ValidatePage(set.page) {
		panic("page is broken.")
	}

	// if librarian slot, advance to real slot
	if set.page.Typ(slot) == Librarian {
		slot++
		ptr = set.page.Key(slot)
	}

	fence := slot == set.page.Cnt

	// if key is found delete it, otherwise ignore request
	found := KeyCmp(ptr, key) == 0
	if found {
		found = !set.page.Dead(slot)
		if found {
			val := *set.page.Value(slot)
			set.page.SetDead(slot, true)
			set.page.Garbage += uint32(1+len(ptr)) + uint32(1+len(val))
			set.page.Act--

			// collapse empty slots beneath the fence
			idx := set.page.Cnt - 1
			for idx > 0 {
				if set.page.Dead(idx) {
					copy(set.page.slotBytes(idx), set.page.slotBytes(idx+1))
					set.page.ClearSlot(set.page.Cnt)
					set.page.Cnt--
				} else {
					break
				}

				idx = set.page.Cnt - 1
			}
			if !ValidatePage(set.page) {
				panic("DeleteKey: page broken!")
			}
		}
	}

	// did we delete a fence key in an upper level?
	if found && lvl > 0 && set.page.Act > 0 && fence {
		if err := tree.fixFence(&set, lvl); err != BLTErrOk {
			return err
		} else {
			return BLTErrOk
		}
	}

	// do we need to collapse root?
	if lvl > 1 && set.latch.pageNo == RootPage && set.page.Act == 1 {
		if err := tree.collapseRoot(&set); err != BLTErrOk {
			return err
		} else {
			return BLTErrOk
		}
	}

	// delete empty page
	if set.page.Act == 0 {
		return tree.deletePage(&set, LockNone)
	}

	if !ValidatePage(set.page) {
		panic("DeleteKey: page is broken.")
	}

	set.latch.dirty = true
	tree.mgr.PageUnlock(LockWrite, set.latch)
	tree.mgr.UnpinLatch(set.latch)
	return BLTErrOk
}

// findNext
//
// advance to next slot
func (tree *BLTree) findNext(set *PageSet, slot uint32) uint32 {
	if slot < set.page.Cnt {
		return slot + 1
	}
	prevLatch := set.latch
	pageNo := GetID(&set.page.Right)
	if pageNo > 0 {
		set.latch = tree.mgr.PinLatch(pageNo, true, &tree.reads, &tree.writes)
		if set.latch != nil {
			set.page = tree.mgr.GetRefOfPageAtPool(set.latch)
		} else {
			return 0
		}
	} else {
		tree.err = BLTErrStruct
		return 0
	}

	// obtain access lock using lock chaining with Access mode
	tree.mgr.PageLock(LockAccess, set.latch)
	tree.mgr.PageUnlock(LockRead, prevLatch)
	tree.mgr.UnpinLatch(prevLatch)
	tree.mgr.PageLock(LockRead, set.latch)
	tree.mgr.PageUnlock(LockAccess, set.latch)
	return 1
}

// FindKey
//
// find unique key or first duplicate key in
// leaf level and return number of value bytes
// or (-1) if not found. Setup key for foundKey
func (tree *BLTree) FindKey(key []byte, valMax int) (ret int, foundKey []byte, foundValue []byte) {
	var set PageSet
	ret = -1

	slot := tree.mgr.PageFetch(&set, key, 0, LockRead, &tree.reads, &tree.writes)
	for ; slot > 0; slot = tree.findNext(&set, slot) {
		ptr := set.page.Key(slot)

		// skip librarian slot place holder
		if set.page.Typ(slot) == Librarian {
			slot++
			ptr = set.page.Key(slot)
		}

		// return actual key found
		foundKey = make([]byte, len(ptr))
		copy(foundKey, ptr)

		keyLen := len(ptr)

		if set.page.Typ(slot) == Duplicate {
			keyLen -= BtId
		}

		// not there if we reach the stopper key
		if slot == set.page.Cnt {
			if GetID(&set.page.Right) == 0 {
				break
			}
		}

		// if key exists, return >= 0 value bytes copied
		// otherwise return (-1)
		if set.page.Dead(slot) {
			continue
		}

		if keyLen == len(key) {
			if KeyCmp(ptr[:keyLen], key) == 0 {
				val := *set.page.Value(slot)
				if valMax > len(val) {
					valMax = len(val)
				}
				foundValue = make([]byte, valMax)
				copy(foundValue, val[:])
				ret = valMax
			}
		}
		break

	}

	tree.mgr.PageUnlock(LockRead, set.latch)
	tree.mgr.UnpinLatch(set.latch)

	return ret, foundKey, foundValue
}

func (tree *BLTree) removeDeletedAndLibrarianSlots(page *Page, slot uint32) {
	// remove deleted keys
	// remove librarian slots

	nxt := tree.mgr.pageDataSize
	max := page.Cnt

	frame := NewPage(tree.mgr.pageDataSize)
	MemCpyPage(frame, page)

	// skip page info and set rest of page to zero
	page.Data = make([]byte, tree.mgr.pageDataSize)
	page.Garbage = 0
	page.Act = 0

	// remove deleted keys and librarian slots
	idx := uint32(0)
	for cnt := uint32(0); cnt < max; {
		cnt++

		if cnt < max && frame.Dead(cnt) {
			continue
		}

		// copy the value across
		val := *frame.Value(cnt)
		nxt -= uint32(len(val) + 1)
		copy(page.Data[nxt:], append([]byte{byte(len(val))}, val[:]...))

		// copy the key across
		key := frame.Key(cnt)
		nxt -= uint32(len(key) + 1)
		copy(page.Data[nxt:], append([]byte{byte(len(key))}, key[:]...))

		// not make librarian slot

		// set up the slot
		idx++
		page.SetKeyOffset(idx, nxt)
		page.SetTyp(idx, frame.Typ(cnt))

		page.SetDead(idx, false)
		page.Act++
	}

	page.Min = nxt
	page.Cnt = idx

	if !ValidatePage(page) {
		panic("cleanPage: page is broken.")
	}
}

// cleanPage
//
// check page for space available,
//
//	clean if necessary and return
//	0 - page needs splitting
//	>0 new slot value
func (tree *BLTree) cleanPage(set *PageSet, keyLen uint8, slot uint32, valLen uint8) uint32 {
	nxt := tree.mgr.pageDataSize
	page := set.page
	max := page.Cnt

	if !ValidatePage(page) {
		panic("cleanPage: page broken!")
	}

	// skip cleanup and proceed to split
	// if there's not enough garbage to bother with.

	//dataSpaceAfterClean := (tree.mgr.pageDataSize - page.Min) + page.Garbage
	dataSpaceAfterClean := uint32(1+keyLen+1+valLen) * (page.Act + 1)

	//afterCleanSize := (tree.mgr.pageDataSize - page.Min) - page.Garbage + (page.Act*2+1)*SlotSize
	afterCleanSize := dataSpaceAfterClean + (page.Act*2+1)*SlotSize
	if int(tree.mgr.pageDataSize)-int(afterCleanSize) < int(tree.mgr.pageDataSize/5) {
		//tree.removeDeletedAndLibrarianSlots(set.page, slot)
		//set.latch.dirty = true
		return 0
	}

	//if page.Min > slot*uint32(SlotSize)+uint32(keyLen)+1+uint32(keyLen)+1 && page.Min > (max+2)*uint32(SlotSize)+uint32(keyLen)+1+uint32(keyLen)+1 {
	//	//fmt.Println("cleanPage return slot. pageNo:", set.latch.pageNo, " slot:", slot, " Cnt:", page.Cnt, " Min:", page.Min)
	//	return slot
	//}

	if dataSpaceAfterClean+(page.Act*2+1)*SlotSize > tree.mgr.pageDataSize {
		// in this case, after cleanup, header space and data space overlaps and it's an illegal state of page
		//tree.removeDeletedAndLibrarianSlots(set.page, slot)
		//set.latch.dirty = true
		return 0
	}

	if page.Min >= (max+2)*SlotSize+uint32(keyLen)+1+uint32(valLen)+1 {
		return slot
	}

	frame := NewPage(tree.mgr.pageDataSize)
	MemCpyPage(frame, page)

	// skip page info and set rest of page to zero
	page.Data = make([]byte, tree.mgr.pageDataSize)
	set.latch.dirty = true
	page.Garbage = 0
	page.Act = 0

	// clean up page first by removing deleted keys
	newSlot := max
	idx := uint32(0)
	for cnt := uint32(0); cnt < max; {
		cnt++
		if cnt == slot {
			if idx == 0 {
				// because librarian slot will not be added
				newSlot = 1
			} else {
				newSlot = idx + 2
			}
		}

		if cnt < max && frame.Dead(cnt) {
			continue
		}

		// copy the value across
		val := *frame.Value(cnt)
		nxt -= uint32(len(val) + 1)
		copy(page.Data[nxt:], append([]byte{byte(len(val))}, val[:]...))

		// copy the key across
		key := frame.Key(cnt)
		nxt -= uint32(len(key) + 1)
		copy(page.Data[nxt:], append([]byte{byte(len(key))}, key[:]...))

		// make a librarian slot
		if idx > 0 {
			idx++
			page.SetKeyOffset(idx, nxt)
			page.SetTyp(idx, Librarian)
			page.SetDead(idx, true)
		}

		// set up the slot
		idx++
		page.SetKeyOffset(idx, nxt)
		page.SetTyp(idx, frame.Typ(cnt))

		if nxt <= idx*SlotSize {
			//log.Printf("cleanPage: nxt overlaps with the slot area!!! nxt: %d, idx: %d, keyLen: %d, valLen: %d, set.latch.pageNo: %d, slot: %d, frame.header: %v, frame.data: %v\n", nxt, idx, keyLen, valLen, set.latch.pageNo, slot, frame.PageHeader, frame.Data)
			panic(fmt.Sprintf("cleanPage: nxt overlaps with the slot area!!! nxt: %d, idx: %d, cnt: %d, keyLen: %d, valLen: %d, set.latch.pageNo: %d, slot: %d, frame.header: %v, frame.data: %v\n", nxt, idx, set.page.Cnt, keyLen, valLen, set.latch.pageNo, slot, frame.PageHeader, frame.Data))
		}

		page.SetDead(idx, frame.Dead(cnt))
		if !page.Dead(idx) {
			page.Act++
		}
	}

	page.Min = nxt
	page.Cnt = idx

	if !ValidatePage(page) {
		panic("cleanPage: page is broken.")
	}

	// see if page has enough space now, or does it need splitting?
	//if tree.mgr.pageDataSize-page.Min < tree.mgr.pageDataSize/5 {
	if page.Min < tree.mgr.pageDataSize/5 {
		//tree.removeDeletedAndLibrarianSlots(set.page, slot)
		//set.latch.dirty = true
		return 0
	} else if page.Min > (idx+2)*SlotSize+uint32(keyLen)+1+uint32(valLen)+1 {
		return newSlot
	} else {
		panic("cleanPage: page is broken.")
	}
}

// splitRoot
//
// split the root and raise the height of the btree
func (tree *BLTree) splitRoot(root *PageSet, right *Latchs) BLTErr {
	var left PageSet
	nxt := tree.mgr.pageDataSize
	var value [BtId]byte
	// save left page fence key for new root
	leftKey := root.page.Key(root.page.Cnt)

	// Obtain an empty page to use, and copy the current
	// root contents into it, e.g. lower keys
	if err := tree.mgr.NewPage(&left, root.page, &tree.reads, &tree.writes); err != BLTErrOk {
		return err
	}

	leftPageNo := left.latch.pageNo
	tree.mgr.UnpinLatch(left.latch)

	// preserve the page info at the bottom
	// of higher keys and set rest to zero
	root.page.Data = make([]byte, tree.mgr.pageDataSize)

	// insert stopper key at top of newroot page
	// and increase the root height
	nxt -= BtId + 1
	PutID(&value, right.pageNo)
	copy(root.page.Data[nxt:], append([]byte{byte(BtId)}, value[:]...))

	nxt -= 2 + 1
	root.page.SetKeyOffset(2, nxt)
	copy(root.page.Data[nxt:], append([]byte{byte(2)}, 0xff, 0xff))

	// insert lower keys page fence key on newroot page as first key
	nxt -= BtId + 1
	PutID(&value, leftPageNo)
	copy(root.page.Data[nxt:], append([]byte{byte(BtId)}, value[:]...))

	nxt -= uint32(len(leftKey)) + 1
	root.page.SetKeyOffset(1, nxt)
	copy(root.page.Data[nxt:], append([]byte{byte(len(leftKey))}, leftKey[:]...))

	PutID(&root.page.Right, 0)
	root.page.Min = nxt
	root.page.Cnt = 2
	root.page.Act = 2
	root.page.Lvl++

	//if root.page.Min < root.page.Cnt*SlotSize {
	//	fmt.Println("splitRoot: need check!")
	//}

	if !ValidatePage(root.page) {
		panic("splitRoot: page broken!")
	}

	// release and unpin root pages
	tree.mgr.PageUnlock(LockWrite, root.latch)
	tree.mgr.UnpinLatch(root.latch)
	tree.mgr.UnpinLatch(right)
	return BLTErrOk
}

// splitPage
//
// split already locked full node; leave it locked.
// @return pool entry for new right page, unlocked
func (tree *BLTree) splitPage(set *PageSet) uint {
	nxt := tree.mgr.pageDataSize
	lvl := set.page.Lvl
	var right PageSet

	// split higher half of keys to frame
	frame := NewPage(tree.mgr.pageDataSize)
	max := set.page.Cnt
	if max <= 1 {
		panic("splitPage: max <= 1")
	}
	cnt := max / 2

	idx := uint32(0)

	for cnt < max {
		cnt++
		if cnt < max || set.page.Lvl > 0 {
			if set.page.Dead(cnt) {
				continue
			}
		}
		value := *set.page.Value(cnt)
		valLen := uint32(len(value))
		nxt -= valLen + 1
		copy(frame.Data[nxt:], append([]byte{byte(valLen)}, value...))

		key := set.page.Key(cnt)
		nxt -= uint32(len(key)) + 1
		copy(frame.Data[nxt:], append([]byte{byte(len(key))}, key[:]...))

		// add librarian slot
		if idx > 0 {
			idx++
			frame.SetKeyOffset(idx, nxt)
			frame.SetTyp(idx, Librarian)
			frame.SetDead(idx, true)
		}

		// add actual slot
		idx++
		frame.SetKeyOffset(idx, nxt)
		frame.SetTyp(idx, set.page.Typ(cnt))

		frame.SetDead(idx, set.page.Dead(cnt))
		if !frame.Dead(idx) {
			frame.Act++
		}
	}

	frame.Bits = tree.mgr.pageBits
	frame.Min = nxt
	frame.Cnt = idx
	frame.Lvl = lvl

	//if (idx+1)*6+(frame.Act-1)*EntrySizeForDebug+3 > tree.mgr.pageDataSize {
	//	//fmt.Println("splitPage: need check!")
	//	panic("splitPage: page broken!")
	//}
	if !ValidatePage(frame) {
		panic("splitPage: page broken!")
	}

	// link right node
	if set.latch.pageNo > RootPage {
		PutID(&frame.Right, GetID(&set.page.Right))
	}

	// get new free page and write higher keys to it.
	if err := tree.mgr.NewPage(&right, frame, &tree.reads, &tree.writes); err != BLTErrOk {
		return 0
	}

	MemCpyPage(frame, set.page)
	set.page.Data = make([]byte, tree.mgr.pageDataSize)
	set.latch.dirty = true

	nxt = tree.mgr.pageDataSize
	set.page.Garbage = 0
	set.page.Act = 0

	max /= 2

	cnt = 0
	idx = 0

	if frame.Typ(max) == Librarian {
		max--
	}

	for cnt < max {
		cnt++
		if frame.Dead(cnt) {
			continue
		}
		value := *frame.Value(cnt)
		valLen := uint32(len(value))
		nxt -= valLen + 1
		copy(set.page.Data[nxt:], append([]byte{byte(valLen)}, value...))

		key := frame.Key(cnt)
		nxt -= uint32(len(key)) + 1
		copy(set.page.Data[nxt:], append([]byte{byte(len(key))}, key[:]...))

		// add librarian slot
		if idx > 0 {
			idx++
			set.page.SetKeyOffset(idx, nxt)
			set.page.SetTyp(idx, Librarian)
			set.page.SetDead(idx, true)
		}

		// add actual slot
		idx++
		set.page.SetKeyOffset(idx, nxt)
		set.page.SetTyp(idx, frame.Typ(cnt))
		set.page.Act++
	}

	PutID(&set.page.Right, right.latch.pageNo)
	set.page.Min = nxt
	set.page.Cnt = idx

	//if (idx+1)*6+(set.page.Act-1)*EntrySizeForDebug+3 > tree.mgr.pageDataSize {
	//	//fmt.Println("splitPage: need check!")
	//	panic("splitPage: page broken!")
	//}

	if !ValidatePage(set.page) {
		panic("splitPage: page broken!")
	}

	if set.page.Cnt == 0 {
		panic("splitPage: Cnt == 0!")
	}

	//fmt.Println("splitPage: Min", set.page.Min, " Cnt:", set.page.Cnt, " Act:", set.page.Act, ", pageNo:", set.latch.pageNo)

	return right.latch.entry
}

// splitKeys
//
// fix keys for newly split page
// call with page locked
// @return unlocked
func (tree *BLTree) splitKeys(set *PageSet, right *Latchs) BLTErr {
	lvl := set.page.Lvl

	// if current page is the root page, split it
	if RootPage == set.latch.pageNo {
		return tree.splitRoot(set, right)
	}

	leftKey := set.page.Key(set.page.Cnt)

	page := tree.mgr.GetRefOfPageAtPool(right)

	rightKey := page.Key(page.Cnt)

	// insert new fences in their parent pages
	tree.mgr.PageLock(LockParent, right)
	tree.mgr.PageLock(LockParent, set.latch)
	tree.mgr.PageUnlock(LockWrite, set.latch)

	// insert new fence for reformulated left block of smaller keys
	var value [BtId]byte
	PutID(&value, set.latch.pageNo)

	if err := tree.InsertKey(leftKey, lvl+1, value, true); err != BLTErrOk {
		return err
	}

	// switch fence for right block of larger keys to new right page
	PutID(&value, right.pageNo)

	if err := tree.InsertKey(rightKey, lvl+1, value, true); err != BLTErrOk {
		return err
	}

	tree.mgr.PageUnlock(LockParent, set.latch)
	tree.mgr.UnpinLatch(set.latch)
	tree.mgr.PageUnlock(LockParent, right)
	tree.mgr.UnpinLatch(right)
	return BLTErrOk
}

// insertSlot install new key and value onto page.
// page must already be checked for adequate space
func (tree *BLTree) insertSlot(
	set *PageSet,
	slot uint32,
	key []byte,
	value [BtId]byte,
	typ SlotType,
	release bool,
) BLTErr {
	//if set.latch.pageNo == 14233 && slot >= 101 {
	//	fmt.Println("insertSlot: need check!")
	//}

	//if set.page.Act*EntrySizeForDebug+set.page.Cnt*8+8+40 > tree.mgr.pageDataSize {
	//	fmt.Println("insertSlot: need check!")
	//}

	//if set.page.Min < slot*SlotSize+uint32(len(key))+1+uint32(len(value))+1 {
	//	fmt.Println("insertSlot: over Min! pageNo:", set.latch.pageNo, " slot:", slot, " Min:", set.page.Min, " Cnt:", set.page.Cnt)
	//	panic("insertSlot: page broken")
	//}

	// if found slot > desired slot and previous slot is a librarian slot, use it
	if slot > 1 {
		if set.page.Typ(slot-1) == Librarian {
			slot--
		}
	}

	// copy value onto page
	set.page.Min -= uint32(len(value)) + 1
	copy(set.page.Data[set.page.Min:], append([]byte{byte(len(value))}, value[:]...))

	// copy key onto page
	set.page.Min -= uint32(len(key) + 1)
	copy(set.page.Data[set.page.Min:], append([]byte{byte(len(key))}, key[:]...))

	// find first empty slot
	idx := slot
	for ; idx < set.page.Cnt; idx++ {
		if set.page.Dead(idx) {
			break
		}
	}

	// now insert key into array before slot
	var librarian uint32
	if idx == set.page.Cnt {
		idx += 2
		set.page.Cnt += 2
		librarian = 2
	} else {
		librarian = 1
	}
	set.latch.dirty = true
	set.page.Act++

	// move slots up to make room for new key
	for idx > slot+librarian-1 {
		set.page.SetDead(idx, set.page.Dead(idx-librarian))
		set.page.SetTyp(idx, set.page.Typ(idx-librarian))
		set.page.SetKeyOffset(idx, set.page.KeyOffset(idx-librarian))
		idx--
	}

	// add librarian slot
	if librarian > 1 {
		set.page.SetKeyOffset(slot, set.page.Min)
		set.page.SetTyp(slot, Librarian)
		set.page.SetDead(slot, true)
		slot++
	}

	// fill in new slot
	set.page.SetKeyOffset(slot, set.page.Min)
	set.page.SetTyp(slot, typ)
	set.page.SetDead(slot, false)

	//if set.latch.pageNo == 14233 && (slot == 101) {
	//	fmt.Println("insertSlot: need check!")
	//}

	if !ValidatePage(set.page) {
		panic("insertSlot: page broken")
	}

	if release {
		tree.mgr.PageUnlock(LockWrite, set.latch)
		tree.mgr.UnpinLatch(set.latch)
	}

	return BLTErrOk
}

// newDup
func (tree *BLTree) newDup() Uid {
	return Uid(atomic.AddUint64(&(&tree.mgr.pageZero).dups, 1))
}

// Attention: length of key should be fixed size
// Note: currently, uniq argument is always true
// InsertKey insert new key into the btree at a given level. either add a new key or update/add an existing one
func (tree *BLTree) InsertKey(key []byte, lvl uint8, value [BtId]byte, uniq bool) BLTErr {
	var slot uint32
	var keyLen uint8
	var set PageSet
	ins := key
	var ptr []byte
	var sequence Uid
	var typ SlotType

	// is this a non-unique index value?
	if uniq {
		typ = Unique
	} else {
		typ = Duplicate
		sequence = tree.newDup()
		var seqBytes [BtId]byte
		PutID(&seqBytes, sequence)
		ins = append(ins, seqBytes[:]...)
	}

	for {
		slot = tree.mgr.PageFetch(&set, key, lvl, LockWrite, &tree.reads, &tree.writes)
		if slot > 0 {
			ptr = set.page.Key(slot)
		} else {
			if tree.err != BLTErrOk {
				tree.err = BLTErrOverflow
			}
			return tree.err
		}

		if !ValidatePage(set.page) {
			panic("InsertKey: page is broken.")
		}
		// if librarian slot == found slot, advance to real slot
		if set.page.Typ(slot) == Librarian {
			if KeyCmp(ptr, key) == 0 {
				slot++
				ptr = set.page.Key(slot)
			}
		}

		keyLen = uint8(len(ptr))

		if set.page.Typ(slot) == Duplicate {
			keyLen -= BtId
		}

		// if inserting a duplicate key or unique key
		//   check for adequate space on the page
		//   and insert the new key before slot.

		if (uniq && (keyLen != uint8(len(ins)) || KeyCmp(ptr, ins) != 0)) || !uniq {
			slot = tree.cleanPage(&set, uint8(len(ins)), slot, BtId)
			if slot == 0 {
				entry := tree.splitPage(&set)
				if entry == 0 {
					return tree.err
				} else if err := tree.splitKeys(&set, &tree.mgr.latchs[entry]); err != BLTErrOk {
					return err
				} else {
					continue
				}
			}
			return tree.insertSlot(&set, slot, ins, value, typ, true)
		}

		// if key already exists, update value and return
		// Note: omit if-block for always true condition
		//val := set.page.Value(slot)
		//if len(val) >= len(value) {
		if set.page.Dead(slot) {
			set.page.Act++
			//if set.page.Typ(slot) == Unique {
			//	reuseSize := uint32(len(key) + 1 + len(value) + 1)
			//	set.page.Garbage -= reuseSize
			//}
		}
		//set.page.Garbage += len(val) - len(value)
		set.latch.dirty = true
		set.page.SetDead(slot, false)
		set.page.SetValue(value[:], slot)

		if !ValidatePage(set.page) {
			panic("InsertKey: page is broken.")
		}
		tree.mgr.PageUnlock(LockWrite, set.latch)
		tree.mgr.UnpinLatch(set.latch)
		return BLTErrOk
		//}

		// new update value doesn't fit in existing value area
		// Note: omit logic for unreachable code
	}

	//return BLTErrOk
}

// iterator methods

// nextKey returns next slot on cursor page
// or slide cursor right into next page
func (tree *BLTree) nextKey(slot uint32) uint32 {
	var set PageSet

	for {
		right := GetID(&tree.cursor.Right)

		for slot < tree.cursor.Cnt {
			slot++
			if tree.cursor.Dead(slot) {
				continue
			} else if right > 0 || (slot < tree.cursor.Cnt) { // skip infinite stopper
				return slot
			} else {
				break
			}
		}

		if right == 0 {
			break
		}

		tree.cursorPage = right

		set.latch = tree.mgr.PinLatch(right, true, &tree.reads, &tree.writes)
		if set.latch != nil {
			set.page = tree.mgr.GetRefOfPageAtPool(set.latch)
		} else {
			return 0
		}

		tree.mgr.PageLock(LockRead, set.latch)
		MemCpyPage(tree.cursor, set.page)
		tree.mgr.PageUnlock(LockRead, set.latch)
		tree.mgr.UnpinLatch(set.latch)
		slot = 0
	}

	tree.err = BLTErrOk
	return 0
}

// startKey cache page of keys into cursor and return starting slot for given key
func (tree *BLTree) startKey(key []byte) uint32 {
	var set PageSet

	// cache page for retrieval
	slot := tree.mgr.PageFetch(&set, key, 0, LockRead, &tree.reads, &tree.writes)
	if slot > 0 {
		MemCpyPage(tree.cursor, set.page)
	} else {
		return 0
	}

	tree.cursorPage = set.latch.pageNo
	tree.mgr.PageUnlock(LockRead, set.latch)
	tree.mgr.UnpinLatch(set.latch)
	return slot
}

// nil argument for lowerKey means no lower bound
// nil argument for upperKey means no upper bound
// ATTENTION: this method call is not atomic with otehr tree operations
func (tree *BLTree) RangeScan(lowerKey []byte, upperKey []byte) (num int, retKeyArr [][]byte, retValArr [][]byte) {
	retKeyArr = make([][]byte, 0)
	retValArr = make([][]byte, 0)
	itrCnt := 0
	var right Uid

	freePinLatchs := func(latch *Latchs) {
		//// page out on parent buffer manager is safe though other threads may be accessing the page
		//// because BLTree doesn't access the parent page's memory directly
		//latch.pin = 0
		tree.mgr.PageUnlock(LockRead, latch)
		tree.mgr.UnpinLatch(latch)
	}

	tmpSet := new(PageSet)
	curSet := new(PageSet)
	curSet.page = NewPage(tree.mgr.pageDataSize)

	//slot := tree.mgr.PageFetch(curSet, lowerKey, 0, LockRead, &tree.reads, &tree.writes)
	slot := tree.mgr.PageFetch(tmpSet, lowerKey, 0, LockRead, &tree.reads, &tree.writes)
	if slot > 0 {
		MemCpyPage(curSet.page, tmpSet.page)
		freePinLatchs(tmpSet.latch)
	} else {
		return 0, *new([][]byte), *new([][]byte)
	}

	getKV := func() bool {
		//slotType := curSet.page.Typ(slot)
		//if slotType != Unique {
		//	return true
		//}
		key := curSet.page.Key(slot)
		val := curSet.page.Value(slot)

		isAboveLower := false
		isBelowUpper := false
		isReachedStopper := false
		// if upperKey is nil, then this condition is always false
		if upperKey != nil && bytes.Compare(key, upperKey) <= 0 {
			isBelowUpper = true
		}
		if lowerKey != nil && bytes.Compare(key, lowerKey) >= 0 {
			isAboveLower = true
		}
		if upperKey == nil {
			isBelowUpper = true
		}
		if lowerKey == nil {
			isAboveLower = true
		}
		if key != nil && len(key) == 2 && key[0] == 0xff && key[1] == 0xff {
			isReachedStopper = true
		}
		if !isAboveLower || !isBelowUpper || isReachedStopper {
			return false
		}

		//if bytes.Compare(key, upperKey)  0 {
		//	return false
		//}

		retKeyArr = append(retKeyArr, key)
		retValArr = append(retValArr, *val)
		itrCnt++
		return true
	}

	readEntriesOfCurSet := func() bool {
		for slot <= curSet.page.Cnt {
			if slot == 0 {
				slot++
			}
			if curSet.page.Dead(slot) {
				slot++
				continue
			} else if curSet.page.Typ(slot) != Unique {
				slot++
				continue
				//} else if right > 0 || slot <= curSet.page.Cnt {
			} else if right > 0 || slot <= curSet.page.Cnt {
				if ok := getKV(); !ok {
					return false
				}
			} else {
				break
			}
			slot++
		}
		return true
	}

	for {
		right = GetID(&curSet.page.Right)

		// reached tail
		if right == 0 {
			readEntriesOfCurSet()
			break
		}

		if ok := readEntriesOfCurSet(); !ok {
			// reached upperKey
			break
		}

		//prevPageLatch := curSet.latch
		//// free lock and unpin
		//freePinLatchs(curSet.latch)

		tmpSet.latch = tree.mgr.PinLatch(right, true, &tree.reads, &tree.writes)
		if tmpSet.latch != nil {
			tmpSet.page = tree.mgr.GetRefOfPageAtPool(tmpSet.latch)
			slot = 0
		} else {
			//panic("PinLatch failed")
			return 0, *new([][]byte), *new([][]byte)
		}
		tree.mgr.PageLock(LockRead, tmpSet.latch)
		MemCpyPage(curSet.page, tmpSet.page)
		freePinLatchs(tmpSet.latch)
	}

	//// free the last page latch
	//freePinLatchs(curSet.latch)
	return itrCnt, retKeyArr, retValArr
}

func (tree *BLTree) GetRangeItr(lowerKey []byte, upperKey []byte) *BLTreeItr {
	elems, keys, vals := tree.RangeScan(lowerKey, upperKey)
	return &BLTreeItr{
		keys:   keys,
		vals:   vals,
		curIdx: 0,
		elems:  uint32(elems),
	}
}

// for debugging
// key length is fixed size with global constant
func ValidatePage(page *Page) bool {
	//actKeys := uint32(0)
	//garbage := uint32(0)
	//for slot := uint32(1); slot <= page.Cnt; slot++ {
	//	switch page.Typ(slot) {
	//	case Unique:
	//		key := page.Key(slot)
	//		//if len(key) != KeySizeForDebug && len(key) != 2 {
	//		//	panic(fmt.Sprintf("ValidatePage: Unique key length is not correct! key: %v\n", key))
	//		//}
	//		val := page.Value(slot)
	//		if len(*val) != BtId && len(*val) != 0 {
	//			panic(fmt.Sprintf("ValidatePage: Unique value length is not correct! val: %v\n", val))
	//		}
	//		isDead := false
	//		if page.Dead(slot) {
	//			isDead = true
	//			garbage += uint32(len(key) + 1 + len(*val) + 1)
	//		}
	//		if (len(*val) != 0 || len(key) == 2) && !isDead {
	//			actKeys++
	//		}
	//	case Librarian:
	//		if !page.Dead(slot) {
	//			panic("ValidatePage: Librarian slot is not dead!")
	//		}
	//		offset := page.KeyOffset(slot)
	//		if offset == 0 {
	//			panic("ValidatePage: Librarian slot key offset is not zero!")
	//		}
	//		if offset > 32767 {
	//			panic("ValidatePage: Librarian slot key offset is too large!")
	//		}
	//		offset = page.ValueOffset(slot)
	//		if offset == 0 {
	//			panic("ValidatePage: Librarian slot value offset is not zero!")
	//		}
	//		if offset > 32767 {
	//			panic("ValidatePage: Librarian slot value offset is too large!")
	//		}
	//	default:
	//		// stopper key
	//		if len(page.Key(slot)) != 2 {
	//			panic("ValidatePage: Stopper key length is not correct!")
	//		}
	//		actKeys++
	//	}
	//}
	//if actKeys != page.Act {
	//	panic(fmt.Sprintf("ValidatePage: Act key count is not correct! %d != %d\n", actKeys, page.Act))
	//}
	////if garbage != page.Garbage {
	////	panic(fmt.Sprintf("validatePage: Garbage value is not collect! %d != %d", garbage, page.Garbage))
	////}
	//if page.Min < page.Cnt*SlotSize {
	//	panic("ValidatePage: Min is not correct!")
	//}
	return true
}
