package sametree_util

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/blink-tree-go/common"
	"github.com/ryogrid/blink-tree-go/storage/page"
	"github.com/ryogrid/blink-tree-go/types"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func PackRIDtoUint32(value *page.RID) uint32 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 4)
	binary.Write(buf1, binary.BigEndian, value.PageId)
	binary.Write(buf2, binary.BigEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[2:], pageIdInBytes[2:])
	copy(pack_buf[:2], slotNumInBytes[2:])
	return binary.BigEndian.Uint32(pack_buf)
}

func UnpackUint32toRID(value uint32) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.BigEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[2:], packedDataInBytes[2:])
	PageId = types.PageID(binary.BigEndian.Uint32(buf))
	copy(buf[2:], packedDataInBytes[:2])
	SlotNum = binary.BigEndian.Uint32(buf)
	ret := new(page.RID)
	ret.PageId = PageId
	ret.SlotNum = SlotNum
	return *ret
}

func PackRIDtoUint64(value *page.RID) uint64 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 8)
	binary.Write(buf1, binary.BigEndian, value.PageId)
	binary.Write(buf2, binary.BigEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[4:], pageIdInBytes[:])
	copy(pack_buf[:4], slotNumInBytes[:])
	return binary.BigEndian.Uint64(pack_buf)
}

func UnpackUint64toRID(value uint64) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.BigEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[:4], packedDataInBytes[4:])
	PageId = types.PageID(binary.BigEndian.Uint32(buf))
	copy(buf[:4], packedDataInBytes[:4])
	SlotNum = binary.BigEndian.Uint32(buf)
	SlotNum = SlotNum
	ret := new(page.RID)
	ret.PageId = PageId
	ret.SlotNum = SlotNum
	return *ret
}

// min length is 1
func GetRandomStr(maxLength int32) *string {
	alphabets :=
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&'(),-./:;<=>?@[]^_`{|}~"
	var len_ int
	len_ = 1 + (rand.Intn(math.MaxInt32))%(int(maxLength)-1)

	s := ""
	for j := 0; j < len_; j++ {
		idx := rand.Intn(52)
		s = s + alphabets[idx:idx+1]
	}

	return &s
}

func RemovePrimitiveFromList[T int32 | float32 | string](list []T, elem T) []T {
	list_ := append(make([]T, 0), list...)
	for i, r := range list {
		if r == elem {
			list_ = append(list[:i], list[i+1:]...)
			break
		}
	}
	return list_
}

func IsContainList[T comparable](list interface{}, searchItem interface{}) bool {
	for _, t := range list.([]T) {
		if t == searchItem.(T) {
			return true
		}
	}
	return false
}

func ChoiceKeyFromMap[T int32 | float32 | string, V int32 | float32 | string | bool | uint32](m map[T]V) T {
	l := len(m)
	i := 0

	index := rand.Intn(l)

	var ans T
	for k := range m {
		if index == i {
			ans = k
			break
		} else {
			i++
		}
	}
	return ans
}

func GetValueForSkipListEntry(val interface{}) uint64 {
	var ret uint64
	switch val.(type) {
	case int32:
		ret = uint64(val.(int32))
	case float32:
		ret = uint64(val.(float32))
	case string:
		ret = uint64(len(val.(string)))
	default:
		panic("unsupported type!")
	}
	return ret
}

func StrideAdd(base interface{}, k interface{}) interface{} {
	switch base.(type) {
	case int32:
		return base.(int32) + k.(int32)
	case float32:
		return base.(float32) + float32(k.(int32))
	case string:
		return base.(string) + "+" + strconv.Itoa(int(k.(int32)))
	default:
		panic("not supported type")
	}
}

func StrideMul(base interface{}, k interface{}) interface{} {
	switch base.(type) {
	case int32:
		return base.(int32) * k.(int32)
	case float32:
		return base.(float32) * float32(k.(int32))
	case string:
		return base.(string) + "*" + strconv.Itoa(int(k.(int32)))
	default:
		panic("not supported type")
	}
}

const SIGN_MASK_BIG uint32 = 0x80000000
const SIGN_MASK_SMALL byte = 0x80

// true = big endian, false = little endian
func getEndian() (ret bool) {
	var i = 0x1
	bs := (*[4]byte)(unsafe.Pointer(&i))
	if bs[0] == 0 {
		return true
	} else {
		return false
	}
}

func SHAssert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func TimeoutPanic() {
	common.RuntimeStack()
	os.Stdout.Sync()
	panic("timeout reached")
}

const flagMask = uint32(1 << ((8 * 4) - 1))

func IsFlagUp(val uint32) bool {
	return val&uint32(flagMask) == uint32(flagMask) || val == 0
}

func SetFlag(val uint32) uint32 {
	return val | uint32(flagMask)
}

func UnsetFlag(val uint32) uint32 {
	return val & (^uint32(flagMask))
}

func MakeSet[T comparable](from []*T) mapset.Set[T] {
	joined := mapset.NewSet[T]()
	for _, f := range from {
		joined.Add(*f)
	}
	return joined
}

func StrSetToString(convSet mapset.Set[string]) string {
	tmpList := convSet.ToSlice()
	sort.Slice(tmpList, func(i, j int) bool {
		return tmpList[i] < tmpList[j]
	})
	return strings.Join(tmpList, ",")
}

func StringToMapset(str string) mapset.Set[string] {
	ret := mapset.NewSet[string]()
	for _, s := range strings.Split(str, ",") {
		ret.Add(s)
	}
	return ret
}

func IsColumnName(v interface{}) bool {
	switch v.(type) {
	case *string:
		return true
	default:
		return false
	}
}

// make deep copied object and set its address to dst pointer type arg
// ex:
// DeepCopy(&dst, &src)
// *src* and *dst* shoud be same type
// attention: must not call this for struct which has private members and interface{} type members
func DeepCopy(dst interface{}, src interface{}) (err error) {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, dst)
	if err != nil {
		return err
	}
	return nil
}
