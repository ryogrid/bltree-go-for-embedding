package blink_tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestBLTree_collapseRoot(t *testing.T) {
	_ = os.Remove("data/collapse_root_test.db")

	type fields struct {
		mgr *BufMgr
	}
	tests := []struct {
		name   string
		fields fields
		want   BLTErr
	}{
		{
			name: "collapse root",
			fields: fields{
				mgr: NewBufMgr("data/collapse_root_test.db", 13, 20, nil, nil),
			},
			want: BLTErrOk,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := NewBLTree(tt.fields.mgr)
			for _, key := range [][]byte{
				{1, 1, 1, 1},
				{1, 1, 1, 2},
			} {
				if err := tree.InsertKey(key, 0, [BtId]byte{1}, true); err != BLTErrOk {
					t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
				}

			}
			if rootAct := tree.mgr.pagePool[RootPage].Act; rootAct != 1 {
				t.Errorf("rootAct = %v, want %v", rootAct, 1)
			}
			if childAct := tree.mgr.pagePool[RootPage+1].Act; childAct != 3 {
				t.Errorf("childAct = %v, want %v", childAct, 3)
			}
			var set PageSet
			set.latch = tree.mgr.PinLatch(RootPage, true, &tree.reads, &tree.writes)
			set.page = tree.mgr.GetRefOfPageAtPool(set.latch)
			if got := tree.collapseRoot(&set); got != tt.want {
				t.Errorf("collapseRoot() = %v, want %v", got, tt.want)
			}

			if rootAct := tree.mgr.pagePool[RootPage].Act; rootAct != 3 {
				t.Errorf("after collapseRoot rootAct = %v, want %v", rootAct, 3)
			}

			if !tree.mgr.pagePool[RootPage+1].Free {
				t.Errorf("after collapseRoot childFree = %v, want %v", false, true)
			}

		})
	}
}

func TestBLTree_cleanPage_full_page(t *testing.T) {
	_ = os.Remove("data/bltree_clean_page.db")
	mgr := NewBufMgr("data/bltree_clean_page.db", 15, HASH_TABLE_ENTRY_CHAIN_LEN*7, nil, nil)
	bltree := NewBLTree(mgr)

	f, err := os.OpenFile("testdata/page_for_clean", os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}

	var data []byte
	for {
		var b byte
		_, err := fmt.Fscanf(f, "%d", &b)
		if err != nil {
			break
		}
		data = append(data, b)
	}
	fmt.Printf("size: %v\n", len(data))

	set := PageSet{
		page:  NewPage(mgr.pageDataSize),
		latch: &Latchs{},
	}
	copy(set.page.Data, data)
	set.page.PageHeader = PageHeader{
		Cnt:     1214,
		Act:     1170,
		Min:     7302,
		Garbage: 6720,
		Bits:    15,
		Free:    false,
		Lvl:     0,
		Kill:    false,
		Right:   [BtId]byte{0, 0, 0, 0, 1, 74},
	}
	res := bltree.cleanPage(&set, 8, 439, BtId)
	if res != 0 {
		t.Errorf("cleanPage() = %v, want %v", res, 0)
	}
}

func TestBLTree_insert_and_find(t *testing.T) {
	mgr := NewBufMgr("data/bltree_insert_and_find.db", 13, 20, nil, nil)
	bltree := NewBLTree(mgr)
	if valLen, _, _ := bltree.FindKey([]byte{1, 1, 1, 1}, BtId); valLen >= 0 {
		t.Errorf("FindKey() = %v, want %v", valLen, -1)
	}

	if err := bltree.InsertKey([]byte{1, 1, 1, 1}, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
	}

	_, foundKey, _ := bltree.FindKey([]byte{1, 1, 1, 1}, BtId)
	if bytes.Compare(foundKey, []byte{1, 1, 1, 1}) != 0 {
		t.Errorf("FindKey() = %v, want %v", foundKey, []byte{1, 1, 1, 1})
	}
}

func TestBLTree_insert_and_find_many(t *testing.T) {
	_ = os.Remove(`data/bltree_insert_and_find_many.db`)
	mgr := NewBufMgr("data/bltree_insert_and_find_many.db", 12, 36, nil, nil)
	bltree := NewBLTree(mgr)

	num := uint64(160000)

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.InsertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.FindKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("FindKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_insert_and_find_concurrently(t *testing.T) {
	_ = os.Remove(`data/insert_and_find_concurrently.db`)
	mgr := NewBufMgr("data/insert_and_find_concurrently.db", 13, HASH_TABLE_ENTRY_CHAIN_LEN*7, nil, nil)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	InsertAndFindConcurrently(t, 7, mgr, keys)
}

func TestBLTree_insert_and_find_concurrently_by_little_endian(t *testing.T) {
	_ = os.Remove(`data/insert_and_find_concurrently_by_little_endian.db`)
	mgr := NewBufMgr("data/insert_and_find_concurrently_by_little_endian.db", 13, HASH_TABLE_ENTRY_CHAIN_LEN*7, nil, nil)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	InsertAndFindConcurrently(t, 7, mgr, keys)
}

func TestBLTree_delete(t *testing.T) {
	mgr := NewBufMgr("data/bltree_delete.db", 13, 20, nil, nil)
	bltree := NewBLTree(mgr)

	key := []byte{1, 1, 1, 1}

	if err := bltree.InsertKey(key, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
	}

	if err := bltree.DeleteKey(key, 0); err != BLTErrOk {
		t.Errorf("DeleteKey() = %v, want %v", err, BLTErrOk)
	}

	if found, _, _ := bltree.FindKey(key, BtId); found != -1 {
		t.Errorf("FindKey() = %v, want %v", found, -1)
	}
}

func TestBLTree_deleteMany(t *testing.T) {
	_ = os.Remove(`data/bltree_delete_many.db`)
	mgr := NewBufMgr("data/bltree_delete_many.db", 13, HASH_TABLE_ENTRY_CHAIN_LEN*7, nil, nil)
	bltree := NewBLTree(mgr)

	keyTotal := 160000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.InsertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
		}
		if i%2 == 0 {
			if err := bltree.DeleteKey(keys[i], 0); err != BLTErrOk {
				t.Errorf("DeleteKey() = %v, want %v", err, BLTErrOk)
			}
		}
	}

	for i := range keys {
		if i%2 == 0 {
			if found, _, _ := bltree.FindKey(keys[i], BtId); found != -1 {
				t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
			}
		} else {
			if found, _, _ := bltree.FindKey(keys[i], BtId); found != 6 {
				t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
			}
		}
	}
}

func TestBLTree_deleteAll(t *testing.T) {
	_ = os.Remove(`data/bltree_delete_all.db`)
	mgr := NewBufMgr("data/bltree_delete_all.db", 13, HASH_TABLE_ENTRY_CHAIN_LEN*7, nil, nil)
	bltree := NewBLTree(mgr)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.InsertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := range keys {
		if err := bltree.DeleteKey(keys[i], 0); err != BLTErrOk {
			t.Errorf("DeleteKey() = %v, want %v", err, BLTErrOk)
		}
		if found, _, _ := bltree.FindKey(keys[i], BtId); found != -1 {
			t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
		}
	}
}

func TestBLTree_deleteManyConcurrently(t *testing.T) {
	_ = os.Remove("data/bltree_delete_many_concurrently.db")
	mgr := NewBufMgr("data/bltree_delete_many_concurrently.db", 12, HASH_TABLE_ENTRY_CHAIN_LEN*7, nil, nil)

	keyTotal := 1600000
	routineNum := 7

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.InsertKey(keys[i], 0, [BtId]byte{}, true); err != BLTErrOk {
					t.Errorf("in goroutine%d InsertKey() = %v, want %v", n, err, BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.DeleteKey(keys[i], 0); err != BLTErrOk {
						t.Errorf("DeleteKey() = %v, want %v", err, BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("FindKey() != -1")
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("FindKey() != 6")
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_restart(t *testing.T) {
	_ = os.Remove(`data/bltree_restart.db`)
	mgr := NewBufMgr("data/bltree_restart.db", 13, 48, nil, nil)
	bltree := NewBLTree(mgr)

	firstNum := uint64(1000)

	for i := uint64(0); i <= firstNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.InsertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	mgr.Close()
	mgr = NewBufMgr("data/bltree_restart.db", 15, 48, nil, nil)
	bltree = NewBLTree(mgr)

	secondNum := uint64(2000)

	for i := firstNum; i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.InsertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.FindKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("FindKey() = %v, want %v", foundKey, bs)
		}
	}
}
