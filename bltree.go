package blink_tree

import (
	"bytes"
	"log"
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
	mgr    BufMgr // buffer manager for thread
	cursor *Page  // cached frame for start/next (never mapped)
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
func NewBLTree(bufMgr BufMgr) *BLTree {
	tree := BLTree{
		mgr: bufMgr,
	}
	tree.cursor = NewPage(bufMgr.GetPageDataSize())

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

	tree.mgr.PageLock(LockParent, set.latch)
	tree.mgr.PageUnlock(LockWrite, set.latch)

	// insert new (now smaller) fence key

	if err := tree.insertKey(leftKey, lvl+1, value, true); err != BLTErrOk {
		return err
	}

	// now delete old fence key
	if err := tree.deleteKey(rightKey, lvl+1); err != BLTErrOk {
		return err
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

		MemCpyPage(root.page, child.page)
		root.latch.dirty = true
		tree.mgr.PageFree(&child)

		if !(root.page.Lvl > 1 && root.page.Act == 1) {
			break
		}
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

	if err := tree.insertKey(higherFence, set.page.Lvl+1, value, true); err != BLTErrOk {
		return err
	}

	// delete old lower key to our node
	if err := tree.deleteKey(lowerFence, set.page.Lvl+1); err != BLTErrOk {
		return err
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

// deleteKey
//
// find and delete key on page by marking delete flag bit
// if page becomes empty, delete it from the btree
func (tree *BLTree) deleteKey(key []byte, lvl uint8) BLTErr {
	var set PageSet

	slot := tree.mgr.PageFetch(&set, key, lvl, LockWrite, &tree.reads, &tree.writes)
	if slot == 0 {
		return tree.err
	}
	ptr := set.page.Key(slot)

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

// findKey
//
// find unique key or first duplicate key in
// leaf level and return number of value bytes
// or (-1) if not found. Setup key for foundKey
func (tree *BLTree) findKey(key []byte, valMax int) (ret int, foundKey []byte, foundValue []byte) {
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

// cleanPage
//
// check page for space available,
//
//	clean if necessary and return
//	0 - page needs splitting
//	>0 new slot value
func (tree *BLTree) cleanPage(set *PageSet, keyLen uint8, slot uint32, valLen uint8) uint32 {
	nxt := tree.mgr.GetPageDataSize()
	page := set.page
	max := page.Cnt

	if page.Min >= (max+2)*SlotSize+uint32(keyLen)+1+uint32(valLen)+1 {
		return slot
	}

	// skip cleanup and proceed to split
	// if there's not enough garbage to bother with.
	afterCleanSize := (tree.mgr.GetPageDataSize() - page.Min) - page.Garbage + (page.Act*2+1)*SlotSize

	if int(tree.mgr.GetPageDataSize())-int(afterCleanSize) < int(tree.mgr.GetPageDataSize()/5) {
		return 0
	}

	frame := NewPage(tree.mgr.GetPageDataSize())
	MemCpyPage(frame, page)

	// skip page info and set rest of page to zero
	page.Data = make([]byte, tree.mgr.GetPageDataSize())
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

		if nxt < idx*SlotSize {
			log.Printf("cleanPage: nxt overlaps with the slot area!!! nxt: %d, idx: %d, keyLen: %d, valLen: %d, slot: %d, frame.header: %v, frame.data: %v\n", nxt, idx, keyLen, valLen, slot, frame.PageHeader, frame.Data)
		}

		page.SetDead(idx, frame.Dead(cnt))
		if !page.Dead(idx) {
			page.Act++
		}
	}

	page.Min = nxt
	page.Cnt = idx

	// see if page has enough space now, or does it need splitting?
	if page.Min >= (idx+2)*SlotSize+uint32(keyLen)+1+uint32(valLen)+1 {
		return newSlot
	}

	return 0
}

// splitRoot
//
// split the root and raise the height of the btree
func (tree *BLTree) splitRoot(root *PageSet, right *Latchs) BLTErr {
	var left PageSet
	nxt := tree.mgr.GetPageDataSize()
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
	root.page.Data = make([]byte, tree.mgr.GetPageDataSize())

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
	nxt := tree.mgr.GetPageDataSize()
	lvl := set.page.Lvl
	var right PageSet

	// split higher half of keys to frame
	frame := NewPage(tree.mgr.GetPageDataSize())
	max := set.page.Cnt
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

	frame.Bits = tree.mgr.GetPageBits()
	frame.Min = nxt
	frame.Cnt = idx
	frame.Lvl = lvl

	// link right node
	if set.latch.pageNo > RootPage {
		PutID(&frame.Right, GetID(&set.page.Right))
	}

	// get new free page and write higher keys to it.
	if err := tree.mgr.NewPage(&right, frame, &tree.reads, &tree.writes); err != BLTErrOk {
		return 0
	}

	MemCpyPage(frame, set.page)
	set.page.Data = make([]byte, tree.mgr.GetPageDataSize())
	set.latch.dirty = true

	nxt = tree.mgr.GetPageDataSize()
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

	if err := tree.insertKey(leftKey, lvl+1, value, true); err != BLTErrOk {
		return err
	}

	// switch fence for right block of larger keys to new right page
	PutID(&value, right.pageNo)

	if err := tree.insertKey(rightKey, lvl+1, value, true); err != BLTErrOk {
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

	if release {
		tree.mgr.PageUnlock(LockWrite, set.latch)
		tree.mgr.UnpinLatch(set.latch)
	}

	return BLTErrOk
}

// newDup
func (tree *BLTree) newDup() Uid {
	return Uid(atomic.AddUint64(&tree.mgr.GetPageZero().dups, 1))
}

// Note: currently, uniq argument is always true
// insertKey insert new key into the btree at a given level. either add a new key or update/add an existing one
func (tree *BLTree) insertKey(key []byte, lvl uint8, value [BtId]byte, uniq bool) BLTErr {
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
				} else if err := tree.splitKeys(&set, &tree.mgr.GetLatchSets()[entry]); err != BLTErrOk {
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
		}
		//set.page.Garbage += len(val) - len(value)
		set.latch.dirty = true
		set.page.SetDead(slot, false)
		set.page.SetValue(value[:], slot)
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

	curSet := new(PageSet)

	slot := tree.mgr.PageFetch(curSet, lowerKey, 0, LockRead, &tree.reads, &tree.writes)

	getKV := func() bool {
		slotType := curSet.page.Typ(slot)
		if slotType != Unique {
			return true
		}
		key := curSet.page.Key(slot)
		val := curSet.page.Value(slot)

		// if upperKey is nil, then this condition is always false
		if bytes.Compare(key, upperKey) < 0 {
			return false
		}

		retKeyArr = append(retKeyArr, key)
		retValArr = append(retValArr, *val)
		itrCnt++
		return true
	}

	freePinLatchs := func(latch *Latchs) {
		tree.mgr.PageUnlock(LockRead, latch)
		tree.mgr.UnpinLatch(latch)
	}

	readEntriesOfCurSet := func() bool {
		for slot < curSet.page.Cnt {
			if curSet.page.Dead(slot) {
				slot++
				continue
			} else {
				if ok := getKV(); !ok {
					return false
				}
			}
			slot++
		}
		return true
	}

	for {
		right := GetID(&curSet.page.Right)

		// the first page is tail or reached tail
		if right == 0 {
			readEntriesOfCurSet()
			break
		}

		if ok := readEntriesOfCurSet(); !ok {
			// reached upperKey
			break
		}

		prevPageLatch := curSet.latch

		curSet.latch = tree.mgr.PinLatch(right, true, &tree.reads, &tree.writes)
		if curSet.latch != nil {
			curSet.page = tree.mgr.GetRefOfPageAtPool(curSet.latch)
		} else {
			panic("PinLatch failed")
		}
		tree.mgr.PageLock(LockRead, curSet.latch)

		// free latch prev page
		freePinLatchs(prevPageLatch)
	}

	// free the last page latch
	freePinLatchs(curSet.latch)
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
