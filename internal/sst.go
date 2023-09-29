package internal

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math"
	"sync"
	"unsafe"
)

var (
	SST_SIZE    = 1024 * 64 //64kb
	HASH_WAY    = 4
	FILTER_SIZE = 1024 * 2
)

type SortSuffixTable struct {
	sync.RWMutex
	header  PageHeader
	Records []Record
}

type Record struct {
	ID     int64
	Suffix string
}
type PageHeader struct {
	Max    string
	Min    string
	bloom  Filter
	Offset int64
	Crc32  int64
	Cap    int64
	Size   int64
	PageID int64
}

func NewSST(ID int64) *SortSuffixTable {
	return &SortSuffixTable{
		header: PageHeader{
			bloom: *New(
				uint64(FILTER_SIZE),
				uint64(HASH_WAY),
				true,
			),
			Offset: int64(SST_SIZE),
			Crc32:  0,
			Size:   0,
			PageID: ID,
			Cap:    int64(SST_SIZE),
		},
		Records: make([]Record, 0),
	}
}

func NewSSTDump(b []byte) *SortSuffixTable {
	n := NewSST(0)

	err := n.Serial(b)

	if err != nil {
		log.Println(err)
		return nil
	}
	return n
}

func (sst *SortSuffixTable) InsertRecord(rc Record) {

	if rc.Suffix < sst.header.Min {
		sst.header.Min = rc.Suffix
		sst.Records = append([]Record{rc}, sst.Records...)
	} else if rc.Suffix > sst.header.Max {
		sst.header.Max = rc.Suffix
		sst.Records = append(sst.Records, rc)
	} else {

		var (
			idx int = -1
		)
		for idx = 0; idx < len(sst.Records); idx++ {
			if sst.Records[idx].Suffix > rc.Suffix {
				break
			}
		}

		if idx == -1 || idx == len(sst.Records) {
			sst.Records = append(sst.Records, rc)
		} else {
			sst.Records = append(sst.Records[:idx], append([]Record{rc}, sst.Records[idx:]...)...)
		}
	}
}

func (sst *SortSuffixTable) RemoveRecord(rc Record) {

	idx := binarySearchRecords(sst.Records, rc.Suffix)
	if idx == -1 {
		return
	}

	sst.Records = append(sst.Records[:idx], sst.Records[idx+1:]...)
}

func (sst *SortSuffixTable) SearchRecord(sf string) *Record {

	if sst.header.Min > sf || sst.header.Max < sf {
		return nil
	}

	if !sst.header.bloom.TestString(sf) {
		return nil
	}

	// binary search

	idx := binarySearchRecords(sst.Records, sf)

	if idx == -1 {
		return nil
	}

	return &sst.Records[idx]
}

func (sst *SortSuffixTable) SearchRangeRecord(low, high string) []Record {

	if high < sst.header.Min || low > sst.header.Max {
		return nil
	}

	if low < sst.header.Min && high > sst.header.Max {
		return sst.Records
	}

	var (
		left  = 0
		right = len(sst.Records) - 1
	)

	for left <= right {
		if sst.Records[left].Suffix < low {
			left++
		}
		if sst.Records[right].Suffix > high {
			right--
		}

		if sst.Records[left].Suffix > low && sst.Records[right].Suffix < high {
			break
		}
	}

	return sst.Records[left:right]

}

func (sst *SortSuffixTable) Dump() (b []byte, err error) {
	b = make([]byte, 0, SST_SIZE) // len 0 cap
	err = sst.header.Dump(b)
	if err != nil {
		return
	}

	w := bytes.NewBuffer(b[len(b):])
	var xlen int32

	offset := sst.header.Cap
	for _, v := range sst.Records {
		xlen = int32(len(v.Suffix))
		offset -= (int64(xlen) + int64(unsafe.Sizeof(int64(0))) + int64(unsafe.Sizeof(int(0))))
		w = bytes.NewBuffer(b[offset:])
		if err = binary.Write(w, binary.BigEndian, v.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, xlen); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, []byte(v.Suffix)); err != nil {
			return
		}

	}

	return
}

func (ph *PageHeader) Dump(b []byte) error {

	w := bytes.NewBuffer(b)
	var err error
	if err = binary.Write(w, binary.BigEndian, ph.PageID); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.Size); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.Cap); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.Crc32); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.Offset); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.bloom.k); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.bloom.n); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.bloom.m); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, ph.bloom.keys); err != nil {
		return err
	}

	var xlen int32
	xlen = int32(len(ph.Max))
	if err = binary.Write(w, binary.BigEndian, xlen); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, []byte(ph.Max)); err != nil {
		return err
	}

	xlen = int32(len(ph.Min))
	if err = binary.Write(w, binary.BigEndian, xlen); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, []byte(ph.Min)); err != nil {
		return err
	}
	return nil
}

func (ph *PageHeader) Serial(b []byte) (int64, error) {
	var (
		i   int64 = 0
		err error
	)

	r := bytes.NewReader(b)

	if err = binary.Read(r, binary.BigEndian, &ph.PageID); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.PageID))

	if err = binary.Read(r, binary.BigEndian, &ph.Size); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.Size))

	if err = binary.Read(r, binary.BigEndian, &ph.Cap); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.Cap))

	if err = binary.Read(r, binary.BigEndian, &ph.Crc32); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.Crc32))

	if err = binary.Read(r, binary.BigEndian, &ph.Offset); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.Offset))

	if err = binary.Read(r, binary.BigEndian, &ph.bloom.k); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.bloom.k))

	if err = binary.Read(r, binary.BigEndian, &ph.bloom.n); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.bloom.n))

	if err = binary.Read(r, binary.BigEndian, &ph.bloom.m); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(ph.bloom.m))

	bs := make([]byte, ph.bloom.m)
	if err = binary.Read(r, binary.BigEndian, bs); err != nil {
		return i, err
	}

	ph.bloom.keys = bs
	ph.bloom.log2m = uint64(math.Log2(float64(ph.bloom.m)))

	i += int64(ph.bloom.m)

	var xlen int32
	if err = binary.Read(r, binary.BigEndian, &xlen); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(xlen))
	ph.Max, err = readString(r, xlen)

	i += int64(len(ph.Max))

	if err != nil {
		return i, err
	}

	if err = binary.Read(r, binary.BigEndian, &xlen); err != nil {
		return i, err
	}
	i += int64(unsafe.Sizeof(xlen))

	ph.Min, err = readString(r, xlen)

	i += int64(len(ph.Max))
	return i, err
}
func (sst *SortSuffixTable) Serial(b []byte) error {

	var (
		rb   int64
		err  error
		xlen int32
	)

	rb, err = sst.header.Serial(b)

	if err != nil {
		return err
	}

	r := bytes.NewReader(b[rb:])

	if err = binary.Read(r, binary.BigEndian, &xlen); err != nil {
		return err
	}

	sst.header.Max, err = readString(r, xlen)

	if err != nil {
		return err
	}

	if err = binary.Read(r, binary.BigEndian, &xlen); err != nil {
		return err
	}

	sst.header.Min, err = readString(r, xlen)

	if err != nil {
		return err
	}
	var record Record

	r = bytes.NewReader(b[sst.header.Offset:])
	for {
		record, err = readRecord(r)
		if err != nil {
			if err == io.EOF {
				return nil
			} else {
				return err
			}
		}

		sst.Records = append([]Record{record}, sst.Records...)
	}
}

func readRecord(r io.Reader) (Record, error) {
	var (
		xlen   int64
		err    error
		record Record
	)

	if err = binary.Read(r, binary.BigEndian, &record.ID); err != nil {
		return record, err
	}

	if err = binary.Read(r, binary.BigEndian, &xlen); err != nil {
		return record, err
	}

	s := make([]byte, xlen)

	if err = binary.Read(r, binary.BigEndian, s); err != nil {
		return record, err
	}
	record.Suffix = string(s)

	return record, err
}

func readString(r io.Reader, size int32) (string, error) {
	var (
		str string
		err error
	)

	b := make([]byte, size)

	err = binary.Read(r, binary.BigEndian, b)

	if err != nil {
		return str, err
	}

	return string(b), err
}

func binarySearchRecords(re []Record, sf string) int {
	var (
		left  int = 0
		mid   int = len(re) / 2
		right int = len(re) - 1
	)

	for left <= right {
		mid = left + (right-left)/2
		if sf > re[mid].Suffix {
			left = mid + 1
		} else if sf < re[mid].Suffix {
			right = mid - 1
		} else {
			return mid
		}
	}

	if mid < right {
		return mid
	}

	return -1
}
