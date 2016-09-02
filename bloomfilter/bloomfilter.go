package BloomFilter

import "sync"
import "fmt"

type BfFilter struct {
	bits         []byte
	filterLocker sync.RWMutex
	n            uint32
}

var masks = []byte{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80}

func NewBloomFilter(num uint32) *BfFilter {
	bf := &BfFilter{}
	bf.Init(num)

	return bf
}

func (v *BfFilter) Init(num uint32) {
	v.filterLocker.Lock()
	v.n = num
	v.bits = make([]byte, num)
	v.filterLocker.Unlock()
}

func (v BfFilter) Error() string {
	return fmt.Sprint("n is ", v.n)
}

func (v *BfFilter) Reinit() {
	v.filterLocker.Lock()
	if v.n == 0 {
		panic(v)
	}

	v.bits = make([]byte, v.n)
	v.filterLocker.Unlock()
}

func bloomFilterhash1(str_s string) uint32 {
	strByte := []byte(str_s)
	var hash uint32
	x := uint32(0)
	for i := 0; i < len(strByte); i++ {
		hash = (hash << 4) + uint32(strByte[i])
		x = uint32(hash & 0xF0000000)
		if x != 0 {
			hash ^= (x >> 24)
		}
		//hash &= ~x
		hash &= ^x
	}
	return hash
}

func bloomFilterhash2(str_s string) uint32 {

	var hash uint32
	strByte := []byte(str_s)
	sLength := len(strByte)
	hash = 5381
	for i := 0; i < sLength; i++ {
		hash = ((hash << 5) + hash) + uint32(strByte[i])
	}
	return hash
}

func bloomFilterhash3(str_s string) uint32 {
	var hash uint32
	strByte := []byte(str_s)
	sLength := len(strByte)
	seed := uint32(131) // 31 131 1313 13131 131313 etc..
	hash = 0
	for i := 0; i < sLength; i++ {
		hash = uint32(hash*seed) + uint32(strByte[i])
	}
	return hash
}

func (v *BfFilter) BloomFilter_Add(key string) {
	h1 := uint32(bloomFilterhash1(key) % (v.n * 8)) //在哪一位置1
	h2 := uint32(bloomFilterhash2(key) % (v.n * 8))
	h3 := uint32(bloomFilterhash3(key) % (v.n * 8))
	idx1 := uint32(h1 >> 3) //具体到char数组的哪个下标
	idx2 := uint32(h2 >> 3)
	idx3 := uint32(h3 >> 3)

	v.filterLocker.Lock()
	v.bits[idx1%v.n] |= masks[h1%8] //将相应位置1
	v.bits[idx2%v.n] |= masks[h2%8]
	v.bits[idx3%v.n] |= masks[h3%8]
	v.filterLocker.Unlock()
}

func (v *BfFilter) BloomFilter_Check(str_s string) bool {
	var result bool
	h1 := uint32(bloomFilterhash1(str_s) % (v.n * 8))
	h2 := uint32(bloomFilterhash2(str_s) % (v.n * 8))
	h3 := uint32(bloomFilterhash3(str_s) % (v.n * 8))
	idx1 := uint32(h1 >> 3)
	idx2 := uint32(h2 >> 3)
	idx3 := uint32(h3 >> 3)

	v.filterLocker.RLock()

	if (v.bits[idx1%v.n]&masks[h1%8]) != 0 && (v.bits[idx2%v.n]&masks[h2%8]) != 0 && (v.bits[idx3%v.n]&masks[h3%8]) != 0 {
		result = true
	} else {
		result = false
	}
	v.filterLocker.RUnlock()
	return result
}

//func (v *BfFilter) BloomFilterCheckAndAdd(str_s string) bool {
//result := v.BloomFilter_Check(str_s)
//if !result {
//	v.BloomFilter_Add(str_s)
//}
//return result
//}

// BloomFilterCheckAndAddDirect
func (v *BfFilter) BloomFilterCheckAndAdd(str_s string) bool {
	var result bool
	h1 := uint32(bloomFilterhash1(str_s) % (v.n * 8))
	h2 := uint32(bloomFilterhash2(str_s) % (v.n * 8))
	h3 := uint32(bloomFilterhash3(str_s) % (v.n * 8))
	idx1 := uint32(h1 >> 3)
	idx2 := uint32(h2 >> 3)
	idx3 := uint32(h3 >> 3)

	v.filterLocker.RLock()
	if (v.bits[idx1%v.n]&masks[h1%8]) != 0 && (v.bits[idx2%v.n]&masks[h2%8]) != 0 && (v.bits[idx3%v.n]&masks[h3%8]) != 0 {
		result = true
	} else {
		result = false
		v.bits[idx1%v.n] |= masks[h1%8] //将相应位置1
		v.bits[idx2%v.n] |= masks[h2%8]
		v.bits[idx3%v.n] |= masks[h3%8]
	}
	v.filterLocker.RUnlock()
	return result
}
