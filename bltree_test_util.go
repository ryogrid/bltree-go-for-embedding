package blink_tree

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func InsertAndFindConcurrently(t *testing.T, routineNum int, mgr *BufMgr, keys [][]byte) {
	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	keyTotal := len(keys)

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

				if _, foundKey, _ := bltree.FindKey(keys[i], BtId); bytes.Compare(foundKey, keys[i]) != 0 {
					t.Errorf("in goroutine%d FindKey() = %v, want %v", n, foundKey, keys[i])
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys concurrently. duration =  %v", keyTotal, time.Since(start))

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
				if _, foundKey, _ := bltree.FindKey(keys[i], BtId); bytes.Compare(foundKey, keys[i]) != 0 {
					t.Errorf("FindKey() = %v, want %v, i = %d", foundKey, keys[i], i)
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}
