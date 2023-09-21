package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"fts/internal/common"
	"os"
	"sort"
	"sync"
)

// ATTETION!!!
// 1.一个数据扩展页的大小最大为512MB，所以键值的理论上限不会超过这个值

type BPTItem interface {
	Bytes() []byte
	Update([]byte)
}

var (
	err   error
	order = 4 //B+树的阶数
)

var (
	INDEX_PAGE       = 1 //索引页
	DATA_PAGE        = 2 //数据页
	EXTEND_DATA_PAGE = 3 //扩展数据页
)

const (
	INDEX_PAGE_DEFAULT_BLOCK_SIZE = 4 * 1024
	DATA_PAGE_DEFAULT_BLOCK_SIZE  = 8 * 1024
)

const (
	INVALID_OFFSET = 0xdeadbeef
	MAX_FREEBLOCKS = 100
)

var HasExistedKeyError = errors.New("hasExistedKey")
var NotFoundKey = errors.New("notFoundKey")
var InvalidDBFormat = errors.New("invalid db format")
var ErrExceed = errors.New("data page overflow")

type OFFTYPE uint64

type BPlusTree struct {
	rootOff        OFFTYPE
	nodePool       *sync.Pool
	freeBlocks     []OFFTYPE
	freePageBlocks [][]OFFTYPE
	file           *os.File
	path           string
	blockSize      uint64
	fileSize       uint64
	indexPageSize  uint64
	dataPageSize   uint64
}

type Node struct {
	// 公有字段
	IsActive bool // 节点所在的磁盘空间是否在当前b+树内
	Keys     []uint64
	Parent   OFFTYPE
	IsLeaf   bool  //是否是叶片
	PageType uint8 //Node类型

	// 索引页特有
	Children []OFFTYPE //子节点的偏移
	Self     OFFTYPE   //自身节点偏移
	// 数据页特有
	Next       OFFTYPE  //前一个叶片
	Prev       OFFTYPE  //后一个叶片
	Records    []string //记录字段
	ExtendPage OFFTYPE  // 扩展的数据页

	// 扩展页字段
	PagePlus uint16 //扩展页倍数，相对数据页大小的N倍
	//KeyOffset uint64 //用来标识分块，为0无效
}

func NewBPlusTree(filename string) (*BPlusTree, error) {
	var (
		fstat os.FileInfo
		err   error
	)

	t := &BPlusTree{}
	t.path = filename
	t.rootOff = INVALID_OFFSET
	t.nodePool = &sync.Pool{
		New: func() interface{} {
			return &Node{}
		},
	}
	// 最大只能分配MAX_FREEBLOCKS块
	t.freeBlocks = make([]OFFTYPE, 0, MAX_FREEBLOCKS)
	if t.file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, err
	}
	t.dataPageSize = DATA_PAGE_DEFAULT_BLOCK_SIZE
	t.indexPageSize = INDEX_PAGE_DEFAULT_BLOCK_SIZE
	// if err = syscall.Statfs(filename, &stat); err != nil {
	// 	return nil, err
	// }
	t.blockSize = uint64(common.GetPlatFormFsBlockSize(filename))
	if t.blockSize == 0 {
		return nil, errors.New("blockSize should be zero")
	}

	if t.blockSize > INDEX_PAGE_DEFAULT_BLOCK_SIZE {
		t.indexPageSize = t.blockSize
	}
	if t.blockSize > DATA_PAGE_DEFAULT_BLOCK_SIZE {
		t.dataPageSize = t.blockSize
	}
	if fstat, err = t.file.Stat(); err != nil {
		return nil, err
	}

	t.fileSize = uint64(fstat.Size())
	if t.fileSize != 0 {
		//读取b+树元信息
		if err = t.restructRootNode(); err != nil {
			return nil, err
		}
		if err = t.checkDiskBlockForFreeNodeList(); err != nil {
			return nil, err
		}
	}

	t.initFreeBlocks()
	return t, nil
}
func (t *BPlusTree) Path() string {
	return t.path
}
func (t *BPlusTree) Close() error {
	if t.file != nil {
		t.file.Sync()
		return t.file.Close()
	}
	return nil
}
func (t *BPlusTree) initFreeBlocks() {
	t.freePageBlocks = make([][]OFFTYPE, 4)
}

// 根节点有可能不一定是在偏移0值处
func (t *BPlusTree) restructRootNode() error {
	var (
		err error
	)
	node := &Node{}
	//寻找第一个二进制格式符合的Node
	//位置为0处的一定是叶子节点!
	for off := uint64(0); off < t.fileSize; off += t.dataPageSize {
		if err = t.seekNode(node, OFFTYPE(off)); err != nil {
			return err
		}
		if node.IsActive {
			break
		}
		//
	}
	// 如果扫描全部文件完毕还没有找到根节点，则此文件格式已损坏
	if !node.IsActive {
		return InvalidDBFormat
	}
	// root 节点的Parent指向 INVALID_OFFSET
	// 如果找到的节点不是Root，找到为止
	for node.Parent != INVALID_OFFSET {
		if err = t.seekNode(node, node.Parent); err != nil {
			return err
		}
	}

	t.rootOff = node.Self

	return nil
}

func (t *BPlusTree) getRate() uint8 {
	return uint8(t.dataPageSize / t.indexPageSize)
}
func (t *BPlusTree) freeDiskPageType(typ uint8, off OFFTYPE) {
	switch typ {
	case uint8(INDEX_PAGE):
		t.freeIndexPage(off)
	case uint8(DATA_PAGE):
		t.freeDataPage(off)
	}
}
func (t *BPlusTree) freeExtendPage(off OFFTYPE, plus int) {
	xl := make([]OFFTYPE, plus)
	for idx := range xl {
		xl[idx] = off + OFFTYPE(idx*int(t.dataPageSize))
	}
	sl := t.freePageBlocks[DATA_PAGE]
	idx := len(sl) - 1
	for idx >= 0 && sl[idx] > off {
		idx--
	}
	idx++
	if idx == len(sl) {
		sl = append(sl, xl...)
	} else {
		sl = append(sl[:idx], append(xl, sl[:idx+1]...)...)
	}

	t.freePageBlocks[DATA_PAGE] = sl
}

// 返还INDEX_PAGE,会保证有序性
func (t *BPlusTree) freeIndexPage(off OFFTYPE) {
	sl := t.freePageBlocks[INDEX_PAGE]
	idx := len(sl) - 1
	for idx >= 0 && sl[idx] > off {
		idx--
	}
	idx++
	if idx == len(sl) {
		sl = append(sl, off)
	} else {
		sl = append(sl[:idx], append([]OFFTYPE{off}, sl[idx+1:]...)...)
	}
	// 二分插入

	// 插入可能引起合并操作
	left := idx - int(t.getRate())
	right := idx + int(t.getRate())
	if left < 0 {
		left = 0
	}
	if right > len(sl) {
		right = len(sl)
	}
	// 合并区间只可能在[left,right)
	for left < right {
		cnt := left
		for cnt < right-1 && sl[cnt]+OFFTYPE(t.indexPageSize) == sl[cnt+1] {
			cnt++
		}
		if cnt-left+1 == int(t.getRate()) {
			//找到了连续的区块，执行合并逻辑
			t.freePageBlocks[DATA_PAGE] = append(t.freePageBlocks[DATA_PAGE], sl[left])
			sl = append(sl[:left], sl[cnt+1:]...)
			return
		}
		left = cnt
	}
}

// 返还DATA_PAGE
func (t *BPlusTree) freeDataPage(off OFFTYPE) {
	sl := t.freePageBlocks[DATA_PAGE]
	idx := len(sl) - 1
	for idx >= 0 && sl[idx] > off {
		idx--
	}
	idx++
	// 二分插入
	if idx == len(sl) {
		sl = append(sl, off)
	} else {
		sl = append(sl[:idx], append([]OFFTYPE{off}, sl[idx+1:]...)...)
	}

}

// 分配一个索引页
func (t *BPlusTree) allocIndexPage() OFFTYPE {
	// 不够就拆分DATA_PAGE
	if len(t.freePageBlocks[INDEX_PAGE]) == 0 {
		off := t.freePageBlocks[DATA_PAGE][0]
		t.freePageBlocks[DATA_PAGE] = t.freePageBlocks[DATA_PAGE][1:]
		for i := uint8(0); i < t.getRate(); i++ {
			t.freePageBlocks[INDEX_PAGE] = append(t.freePageBlocks[INDEX_PAGE], off+OFFTYPE(uint64(i)*t.indexPageSize))
		}
	}

	off := t.freePageBlocks[INDEX_PAGE][0]
	t.freePageBlocks[INDEX_PAGE] = t.freePageBlocks[INDEX_PAGE][1:]
	return off
}

// 分配一个数据页
func (t *BPlusTree) allocDataPage() OFFTYPE {
	// 有可能溢出
	off := t.freePageBlocks[DATA_PAGE][0]
	t.freePageBlocks[DATA_PAGE] = t.freePageBlocks[DATA_PAGE][1:]
	return off
}
func (t *BPlusTree) allocPage(typ uint8) OFFTYPE {
	switch typ {
	case uint8(INDEX_PAGE):
		return t.allocIndexPage()
	case uint8(DATA_PAGE):
	case uint8(EXTEND_DATA_PAGE):
		return t.allocDataPage()
	}
	return INVALID_OFFSET
}

func (t *BPlusTree) allocExtendPage(plus uint16) OFFTYPE {
	if plus < 255 {
		// 有当前freeList申请
		idx := 0
		if len(t.freePageBlocks[DATA_PAGE]) < int(plus) {
			t.paddingFreeBlocks(2 * int(plus)) //填充至两倍，保证至少本次能合并出大页面
		}
		pos := 0
		for pos <= len(t.freePageBlocks[DATA_PAGE]) {
			cnt := pos

			for cnt < len(t.freePageBlocks[DATA_PAGE])-1 {
				if t.freePageBlocks[DATA_PAGE][cnt]+OFFTYPE(t.blockSize) != t.freePageBlocks[DATA_PAGE][cnt+1] {
					break
				}
			}
			if pos-cnt == int(plus)-1 {
				idx = pos
				break
			}
			pos = cnt + 1
		}

		if idx != 0 {
			off := t.freePageBlocks[DATA_PAGE][idx]
			t.freePageBlocks[DATA_PAGE] = append(t.freePageBlocks[DATA_PAGE][:idx], t.freePageBlocks[DATA_PAGE][idx+int(plus):]...)
			return off
		}
	} else {
		// 直接扩展文件大小
		pos := t.fileSize
		t.fileSize += uint64(plus) * t.dataPageSize
		return OFFTYPE(pos)
	}

	return INVALID_OFFSET
}

// 调整合并磁盘空间
func (t *BPlusTree) adjustFreeBlockList() {
	idx := []int{}
	i := 0
	for i < len(t.freeBlocks) {
		cnt := i
		for {
			if cnt == len(t.freeBlocks)-1 {
				cnt++
				break
			}
			if t.freeBlocks[cnt]+OFFTYPE(t.indexPageSize) != t.freeBlocks[cnt+1] ||
				cnt-i == int(t.getRate()) {
				break
			}
			cnt++
		}
		if cnt-i == int(t.getRate()) {
			idx = append(idx, i) // merge page
		}
		i = cnt
	}
	plus := int(t.getRate())
	t.freePageBlocks[0] = nil
	// 合并data_page
	for _, v := range idx {
		t.freePageBlocks[DATA_PAGE] = append(t.freePageBlocks[DATA_PAGE], t.freeBlocks[v])
		//t.freeBlocks = append(t.freeBlocks[:v], t.freeBlocks[v+plus:]...)
	}
	sl := []OFFTYPE{}
	//剔除合并成大片磁盘空间的freeBlock
	last := 0
	for _, v := range idx {
		if v == 0 {
			sl = append(t.freeBlocks[:v], t.freeBlocks[v+plus:]...)
		} else {
			sl = append(t.freeBlocks[last:v], t.freeBlocks[v+plus:]...)
		}
		last = v + plus
	}
	// 合并index_page
	// for _, v := range sl {
	// 	t.freePageBlocks[INDEX_PAGE] = append(t.freePageBlocks[INDEX_PAGE], v)
	// }
	t.freePageBlocks[INDEX_PAGE] = append(t.freePageBlocks[INDEX_PAGE], sl...)
	// 清空t.freeBlocks
	t.freeBlocks = make([]OFFTYPE, 0)
}

// 调整步长
func (t *BPlusTree) advanceOffset(n *Node) int64 {
	switch n.PageType {
	case uint8(INDEX_PAGE):
		return int64(t.indexPageSize)
	case uint8(DATA_PAGE):
		return int64(t.dataPageSize)
	case uint8(EXTEND_DATA_PAGE):
		return int64(uint64(n.PagePlus) * t.dataPageSize)
	default:
		return -1
	}
}

// 向磁盘申请空间，将当前freeBlockList并补充至limit
func (t *BPlusTree) paddingFreeBlocks(limit int) {
	if limit > MAX_FREEBLOCKS {
		limit = MAX_FREEBLOCKS
	}
	defer t.adjustFreeBlockList()
	// 如果当前还有较多数据页，则不分配
	if len(t.freePageBlocks[DATA_PAGE]) > limit/2 {
		return
	}

	bs := t.indexPageSize
	// 将next 取到t.filesize 最接近t.indexpagesize倍数的值  4094 -> 4096 0 -> 0 1->4096
	next_file := ((t.fileSize + t.indexPageSize - 1) / t.indexPageSize) * t.indexPageSize
	for len(t.freeBlocks) < limit {
		t.freeBlocks = append(t.freeBlocks, OFFTYPE(next_file))
		next_file += bs
	}
	// 重新扩张文件大小至整数倍blocksize
	t.fileSize = next_file
}

// 校验已存在的数据文件中的Node
// 收回未使用的Block
func (t *BPlusTree) checkDiskBlockForFreeNodeList() error {
	var (
		err error
	)
	node := &Node{}
	bs := t.dataPageSize
	//原有文件上如果有未分配的Block，加入freeBlocks
	// len(t.freeBlocks) < MAX_FREEBLOCKS
	// 这里删除了这个判断因为 可能删除的扩展页合并成索引页面会轻易超过限制
	for off := uint64(0); off < t.fileSize; off += bs {
		if off+bs > t.fileSize {
			break
		}
		if err = t.seekNode(node, OFFTYPE(off)); err != nil {
			return err
		}

		if !node.IsActive {
			t.freeBlocks = append(t.freeBlocks, OFFTYPE(off))
			bs = t.indexPageSize
		} else {
			bs = uint64(t.advanceOffset(node))
		}
	}

	t.paddingFreeBlocks(MAX_FREEBLOCKS)
	return nil
}

func (t *BPlusTree) initNodeForUsage(node *Node) {
	node.IsActive = true
	node.Children = nil
	node.PageType = 0
	node.Self = INVALID_OFFSET
	node.Next = INVALID_OFFSET
	node.Prev = INVALID_OFFSET
	node.Parent = INVALID_OFFSET
	node.ExtendPage = INVALID_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
	node.PagePlus = 0
}

func (t *BPlusTree) clearNodeForUsage(node *Node) {
	node.IsActive = false
	node.Children = nil
	node.Self = INVALID_OFFSET
	node.Next = INVALID_OFFSET
	node.Prev = INVALID_OFFSET
	node.Parent = INVALID_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
	node.ExtendPage = INVALID_OFFSET
	node.PageType = 0
	node.PagePlus = 0
}

// 序列化，将文件off处的字节序列化至 Node中
func (t *BPlusTree) seekNode(node *Node, off OFFTYPE) error {
	if node == nil {
		return fmt.Errorf("cant use nil for seekNode")
	}
	t.clearNodeForUsage(node)

	var err error
	buf := make([]byte, 8)
	if n, err := t.file.ReadAt(buf, int64(off)); err != nil {
		return err
	} else if uint64(n) != 8 {
		return fmt.Errorf("readat %d from %s, expected len = %d but get %d", off, t.file.Name(), 4, n)
	}
	bs := bytes.NewBuffer(buf)

	dataLen := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &dataLen); err != nil {
		return err
	}
	if uint64(dataLen)+8 > t.blockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen)+4, t.blockSize)
	}

	buf = make([]byte, dataLen)
	if n, err := t.file.ReadAt(buf, int64(off)+8); err != nil {
		return err
	} else if uint64(n) != uint64(dataLen) {
		return fmt.Errorf("readat %d from %s, expected len = %d but get %d", int64(off)+4, t.file.Name(), dataLen, n)
	}

	bs = bytes.NewBuffer(buf)

	// IsActive
	if err = binary.Read(bs, binary.LittleEndian, &node.IsActive); err != nil {
		return err
	}
	// PageType
	if err = binary.Read(bs, binary.LittleEndian, &node.PageType); err != nil {
		return err
	}
	// // ExtendPage
	// if err = binary.Read(bs, binary.LittleEndian, &node.ExtendPage); err != nil {
	// 	return err
	// }

	// Children
	childCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &childCount); err != nil {
		return err
	}
	node.Children = make([]OFFTYPE, childCount)
	for i := uint8(0); i < childCount; i++ {
		child := uint64(0)
		if err = binary.Read(bs, binary.LittleEndian, &child); err != nil {
			return err
		}
		node.Children[i] = OFFTYPE(child)
	}

	// Self
	self := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &self); err != nil {
		return err
	}
	node.Self = OFFTYPE(self)

	// Next
	next := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &next); err != nil {
		return err
	}
	node.Next = OFFTYPE(next)

	// Prev
	prev := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &prev); err != nil {
		return err
	}
	node.Prev = OFFTYPE(prev)

	// Parent
	parent := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &parent); err != nil {
		return err
	}
	node.Parent = OFFTYPE(parent)
	// IsLeaf
	if err = binary.Read(bs, binary.LittleEndian, &node.IsLeaf); err != nil {
		return err
	}
	// ExtendPage
	extend := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &extend); err != nil {
		return err
	}
	node.ExtendPage = OFFTYPE(extend)
	// Keys
	keysCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &keysCount); err != nil {
		return err
	}
	node.Keys = make([]uint64, keysCount)
	for i := uint8(0); i < keysCount; i++ {
		if err = binary.Read(bs, binary.LittleEndian, &node.Keys[i]); err != nil {
			return err
		}
	}

	// Records
	recordCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &recordCount); err != nil {
		return err
	}
	node.Records = make([]string, recordCount)
	for i := uint8(0); i < recordCount; i++ {
		l := uint8(0)
		if err = binary.Read(bs, binary.LittleEndian, &l); err != nil {
			return err
		}
		v := make([]byte, l)
		if err = binary.Read(bs, binary.LittleEndian, &v); err != nil {
			return err
		}
		node.Records[i] = string(v)
	}
	// 扩展数据页
	if node.ExtendPage != 0 && node.ExtendPage != INVALID_OFFSET {
		nn, _ := t.newMappingNodeFromPool(INVALID_OFFSET)

		err = t.seekNode(nn, node.ExtendPage)
		if err == nil {
			return err
		}

		node.Keys = append(node.Keys, nn.Keys...)
		node.Records = append(node.Records, nn.Records...)
	}
	return nil
}

func (t *BPlusTree) flushNodesAndPutNodesPool(nodes ...*Node) error {
	for _, n := range nodes {
		if err := t.flushNodeAndPutNodePool(n); err != nil {
			return err
		}
	}
	return err
}

// 将改动变更到磁盘上
func (t *BPlusTree) flushNodeAndPutNodePool(n *Node) error {
	if err := t.flushNode(n); err != nil {
		if err == ErrExceed { //数据超出正常数据页大小
			return t.exceedFlushNode(n)
		}

		return err
	}
	t.putNodePool(n)
	return nil
}

// 规划Node会内存池
func (t *BPlusTree) putNodePool(n *Node) {
	t.nodePool.Put(n)
}

func (t *BPlusTree) exceedFlushNode(n *Node) error {
	var (
		new_node *Node
		err      error
	)
	bs := bytes.NewBuffer(make([]byte, 0))
	// IsActive
	if err = binary.Write(bs, binary.LittleEndian, n.IsActive); err != nil {
		return nil
	}
	// PageType
	if err = binary.Write(bs, binary.LittleEndian, n.PageType); err != nil {
		return err
	}
	// Children
	childCount := uint8(len(n.Children))
	if err = binary.Write(bs, binary.LittleEndian, childCount); err != nil {
		return err
	}
	for _, v := range n.Children {
		if err = binary.Write(bs, binary.LittleEndian, uint64(v)); err != nil {
			return err
		}
	}

	// Self
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Self)); err != nil {
		return err
	}

	// Next
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Next)); err != nil {
		return err
	}

	// Prev
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Prev)); err != nil {
		return err
	}

	// Parent
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Parent)); err != nil {
		return err
	}
	// IsLeaf
	if err = binary.Write(bs, binary.LittleEndian, n.IsLeaf); err != nil {
		return err
	}

	// 统计针对keys的分割位置
	stop := 0
	leftSize := 0
	lens := len(bs.Bytes())
	lens += 8 // datalen
	lens += 1 // keysCount 一个节点最多只能存255个keys
	lens += 1 // recordsCount
	for idx := range n.Keys {
		prev := lens
		lens += 8 //uint64
		lens += 4 //record lens int
		lens += len(n.Records[idx])
		if lens > int(t.dataPageSize) && stop == 0 {
			stop = idx
			leftSize = prev + 8 // ExtendPage
		}
	}

	right := lens - leftSize + 1024 // 1024 分给没录的元数据，如Self,Parent等等
	// 向上取整至 数据页大小的整数倍
	rightSize := (right + int(t.dataPageSize) - 1) / int(t.dataPageSize) * int(t.dataPageSize)
	// 倍率
	plus := uint16(rightSize / int(t.dataPageSize))
	// 判断原有的扩展页是否能够放下新数据
	if n.ExtendPage != INVALID_OFFSET {
		new_node, err = t.newMappingNodeFromPool(n.ExtendPage)
		if new_node.PagePlus < plus {
			// 原来的扩展页空间也不够了
			t.freeExtendPage(new_node.Self, int(new_node.PagePlus))
			new_node, err = t.newExtendNodeFromDisk(plus)
		}
	} else {
		new_node, err = t.newExtendNodeFromDisk(plus)
		new_node.Parent = n.Self
		n.ExtendPage = new_node.Self
	}
	new_node.Records = n.Records[stop:]
	new_node.Keys = n.Keys[stop:]
	defer func() {
		err = t.flushNode(new_node) //刷新扩展页
	}()
	// ExtendPage
	if err = binary.Write(bs, binary.LittleEndian, n.ExtendPage); err != nil {
		return err
	}

	// Keys
	keysCount := uint8(len(n.Keys[:stop]))
	if err = binary.Write(bs, binary.LittleEndian, keysCount); err != nil {
		return err
	}
	for _, v := range n.Keys[:stop] {
		if err = binary.Write(bs, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	// Record
	recordCount := uint8(len(n.Records[:stop]))
	if err = binary.Write(bs, binary.LittleEndian, recordCount); err != nil {
		return err
	}
	for _, v := range n.Records[:stop] {
		if err = binary.Write(bs, binary.LittleEndian, len([]byte(v))); err != nil {
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	dataLen := len(bs.Bytes())
	tmpbs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(tmpbs, binary.LittleEndian, uint64(dataLen)); err != nil {
		return err
	}
	var length int
	data := append(tmpbs.Bytes(), bs.Bytes()...)
	if length, err = t.file.WriteAt(data, int64(n.Self)); err != nil {
		return err
	} else if len(data) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(n.Self), t.file.Name(), len(data), length)
	}
	return nil
}

// 将Node刷新至磁盘
func (t *BPlusTree) flushNode(n *Node) error {
	if n == nil {
		return fmt.Errorf("flushNode == nil")
	}
	if t.file == nil {
		return fmt.Errorf("flush node into disk, but not open file")
	}
	// 不负责含有扩展页的节点刷新
	if n.ExtendPage != INVALID_OFFSET {
		return ErrExceed
	}
	var (
		length int
		err    error
	)

	bs := bytes.NewBuffer(make([]byte, 0))

	// IsActive
	if err = binary.Write(bs, binary.LittleEndian, n.IsActive); err != nil {
		return nil
	}
	// PageType
	if err = binary.Write(bs, binary.LittleEndian, n.PageType); err != nil {
		return err
	}
	// Children
	childCount := uint8(len(n.Children))
	if err = binary.Write(bs, binary.LittleEndian, childCount); err != nil {
		return err
	}
	for _, v := range n.Children {
		if err = binary.Write(bs, binary.LittleEndian, uint64(v)); err != nil {
			return err
		}
	}

	// Self
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Self)); err != nil {
		return err
	}

	// Next
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Next)); err != nil {
		return err
	}

	// Prev
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Prev)); err != nil {
		return err
	}

	// Parent
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Parent)); err != nil {
		return err
	}
	// IsLeaf
	if err = binary.Write(bs, binary.LittleEndian, n.IsLeaf); err != nil {
		return err
	}
	// ExtendPage
	if err = binary.Write(bs, binary.LittleEndian, n.ExtendPage); err != nil {
		return err
	}
	// Keys
	keysCount := uint8(len(n.Keys))
	if err = binary.Write(bs, binary.LittleEndian, keysCount); err != nil {
		return err
	}
	for _, v := range n.Keys {
		if err = binary.Write(bs, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	// Record
	recordCount := uint8(len(n.Records))
	if err = binary.Write(bs, binary.LittleEndian, recordCount); err != nil {
		return err
	}
	for _, v := range n.Records {
		if err = binary.Write(bs, binary.LittleEndian, uint8(len([]byte(v)))); err != nil {
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	dataLen := len(bs.Bytes())
	// 仅对正常的数据页判断是否溢出
	if uint64(dataLen)+8 > t.dataPageSize && n.PageType == uint8(DATA_PAGE) {
		return ErrExceed
		// return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen)+4, t.blockSize)
	}

	tmpbs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(tmpbs, binary.LittleEndian, uint64(dataLen)); err != nil {
		return err
	}

	data := append(tmpbs.Bytes(), bs.Bytes()...)
	if length, err = t.file.WriteAt(data, int64(n.Self)); err != nil {
		return err
	} else if len(data) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(n.Self), t.file.Name(), len(data), length)
	}
	return nil
}

// 如果指定off为非法值，则返回一个空的Node对象，如果off不是非法值。则从磁盘读取这个Node
func (t *BPlusTree) newMappingNodeFromPool(off OFFTYPE) (*Node, error) {
	node := t.nodePool.Get().(*Node)
	t.initNodeForUsage(node)
	if off == INVALID_OFFSET {
		return node, nil
	}
	t.clearNodeForUsage(node)
	if err := t.seekNode(node, off); err != nil {
		return nil, err
	}
	return node, nil
}
func (t *BPlusTree) newIndexNodeFromDisk() (*Node, error) {
	var (
		node *Node
	)

	node = t.nodePool.Get().(*Node)

	if len(t.freePageBlocks[DATA_PAGE]) == 0 &&
		len(t.freePageBlocks[INDEX_PAGE]) == 0 {
		t.paddingFreeBlocks(MAX_FREEBLOCKS / 2) //申请一半上限的索引页
	}

	off := t.allocIndexPage()

	t.initNodeForUsage(node)
	node.PageType = uint8(INDEX_PAGE)
	node.Self = off

	return node, nil
}
func (t *BPlusTree) newExtendNodeFromDisk(plus uint16) (*Node, error) {
	var (
		node *Node
		err  error
	)
	node = t.nodePool.Get().(*Node)
	off := t.allocExtendPage(plus)
	t.initNodeForUsage(node)

	node.Self = off
	node.PagePlus = plus
	node.IsLeaf = true
	node.PageType = uint8(EXTEND_DATA_PAGE)
	return node, err
}
func (t *BPlusTree) newDataNodeFromDisk() (*Node, error) {
	var (
		node *Node
	)

	node = t.nodePool.Get().(*Node)

	if len(t.freePageBlocks[DATA_PAGE]) == 0 {
		t.paddingFreeBlocks(MAX_FREEBLOCKS / 2) //申请一半上限的索引页
	}

	off := t.allocDataPage()

	t.initNodeForUsage(node)
	node.PageType = uint8(DATA_PAGE)
	node.Self = off
	node.IsLeaf = true
	return node, nil
}
func (t *BPlusTree) newTypeNodeFromDisk(typ uint8) (*Node, error) {
	switch typ {
	case uint8(INDEX_PAGE):
		return t.newIndexNodeFromDisk()
	case uint8(DATA_PAGE):
		return t.newDataNodeFromDisk()
	case uint8(EXTEND_DATA_PAGE):
		return nil, fmt.Errorf("use newExtendNodeFromDisk INSTAED!")
	}
	return nil, fmt.Errorf("unknown type node")
}

// 从磁盘分配一个节点
// deprecated
func (t *BPlusTree) newNodeFromDisk() (*Node, error) {
	var (
		node *Node
		err  error
	)
	node = t.nodePool.Get().(*Node)
	// 一个Node分一个Block
	if len(t.freeBlocks) > 0 {
		off := t.freeBlocks[0]
		t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
		t.initNodeForUsage(node)
		node.Self = off
		return node, nil
	}
	// 补充freeblocklist
	if err = t.checkDiskBlockForFreeNodeList(); err != nil {
		return nil, err
	}
	if len(t.freeBlocks) > 0 {
		off := t.freeBlocks[0]
		t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
		t.initNodeForUsage(node)
		node.Self = off
		return node, nil
	}
	return nil, fmt.Errorf("can't not alloc more node")
}

func (t *BPlusTree) putFreeBlocks(n *Node) {
	if len(t.freeBlocks) >= MAX_FREEBLOCKS {
		return
	}
	if n.PageType == uint8(EXTEND_DATA_PAGE) {
		t.freeExtendPage(n.Self, int(n.PagePlus))
	} else {
		t.freeDiskPageType(n.PageType, n.Self)
	}
}

func (t *BPlusTree) Find(key uint64) (string, error) {
	var (
		node *Node
		err  error
	)

	if t.rootOff == INVALID_OFFSET {
		return "", nil
	}

	if node, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return "", err
	}
	//这里只负责找到key应该出现在哪个leaf上
	if err = t.findLeaf(node, key); err != nil {
		return "", err
	}
	defer t.putNodePool(node)

	for i, nkey := range node.Keys {
		if nkey == key {
			return node.Records[i], nil
		}
	}
	return "", NotFoundKey
}

// 锁定key应该出现在哪个leaf上，不管key是否存在
func (t *BPlusTree) findLeaf(node *Node, key uint64) error {
	var (
		err  error
		root *Node
	)

	c := t.rootOff
	if c == INVALID_OFFSET {
		return nil
	}
	//加载root节点
	if root, err = t.newMappingNodeFromPool(c); err != nil {
		return err
	}
	defer t.putNodePool(root)

	*node = *root
	// 沿着根节点向下搜索
	for !node.IsLeaf {
		// 寻找一个有序切片中第一个大于等于key的索引
		// 如果没找到，返回切片的长度
		// [1 3 7 9] @6 idx=2
		idx := sort.Search(len(node.Keys), func(i int) bool {
			return key <= node.Keys[i]
		})
		if idx == len(node.Keys) {
			idx = len(node.Keys) - 1
		}
		if err = t.seekNode(node, node.Children[idx]); err != nil {
			return err
		}
	}
	return nil
}

func cut(length int) int {
	return (length + 1) / 2
}

// Node的Keys是排列有序
func insertKeyValIntoLeaf(n *Node, key uint64, rec string) (int, error) {
	//二分搜索第一个位置的Key大于key
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return key <= n.Keys[i]
	})
	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, HasExistedKeyError
	}
	//二分插入
	n.Keys = append(n.Keys, key)
	n.Records = append(n.Records, rec)
	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Records[i] = n.Records[i-1]
	}
	n.Keys[idx] = key
	n.Records[idx] = string(rec)
	return idx, nil
}

func insertKeyValIntoNode(n *Node, key uint64, child OFFTYPE) (int, error) {
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return key <= n.Keys[i]
	})
	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, HasExistedKeyError
	}

	n.Keys = append(n.Keys, key)
	n.Children = append(n.Children, child)
	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Children[i] = n.Children[i-1]
	}
	n.Keys[idx] = key
	n.Children[idx] = child
	return idx, nil
}

func removeKeyFromLeaf(leaf *Node, idx int) {
	tmpKeys := append([]uint64{}, leaf.Keys[idx+1:]...)
	leaf.Keys = append(leaf.Keys[:idx], tmpKeys...)

	tmpRecords := append([]string{}, leaf.Records[idx+1:]...)
	leaf.Records = append(leaf.Records[:idx], tmpRecords...)
}

func removeKeyFromNode(node *Node, idx int) {
	tmpKeys := append([]uint64{}, node.Keys[idx+1:]...)
	node.Keys = append(node.Keys[:idx], tmpKeys...)

	tmpChildren := append([]OFFTYPE{}, node.Children[idx+1:]...)
	node.Children = append(node.Children[:idx], tmpChildren...)
}

// 分裂叶片成两片，更改底部链表的指向
// 此函数并没有将left,new_leaf的改动刷新至磁盘中,需要caller手动刷新
func (t *BPlusTree) splitLeafIntoTowleaves(leaf *Node, new_leaf *Node) error {
	var (
		i, split int
	)
	split = cut(order)

	for i = split; i <= order; i++ {
		new_leaf.Keys = append(new_leaf.Keys, leaf.Keys[i])
		new_leaf.Records = append(new_leaf.Records, leaf.Records[i])
	}

	// adjust relation
	leaf.Keys = leaf.Keys[:split]
	leaf.Records = leaf.Records[:split]

	new_leaf.Next = leaf.Next
	leaf.Next = new_leaf.Self
	new_leaf.Prev = leaf.Self

	new_leaf.Parent = leaf.Parent

	if new_leaf.Next != INVALID_OFFSET {
		// 如果new leaf有下一个指向，更新下一个节点的prev
		var (
			nextNode *Node
			err      error
		)
		if nextNode, err = t.newMappingNodeFromPool(new_leaf.Next); err != nil {
			return err
		}
		nextNode.Prev = new_leaf.Self
		if err = t.flushNodesAndPutNodesPool(nextNode); err != nil {
			return err
		}
	}

	return err
}

func (t *BPlusTree) insertIntoLeaf(key uint64, rec string) error {
	var (
		leaf     *Node
		err      error
		idx      int
		new_leaf *Node
	)

	if leaf, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}
	// 找到一个叶子节点
	if err = t.findLeaf(leaf, key); err != nil {
		return err
	}
	//idx key插入位置
	if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
		return err
	}

	// update the last key of parent's if necessary
	// 如果插入的key大于当前b+树的最大数值会发生这种情况，需要沿叶子节点向上更新
	if err = t.mayUpdatedLastParentKey(leaf, idx); err != nil {
		return err
	}

	// insert key/val into leaf
	// 如果叶子节点小于当前b树的阶数
	if len(leaf.Keys) <= order {
		return t.flushNodeAndPutNodePool(leaf)
	}
	// 叶子节点已满，执行分裂逻辑
	// split leaf so new leaf node
	if new_leaf, err = t.newDataNodeFromDisk(); err != nil {
		return err
	}
	new_leaf.IsLeaf = true
	if err = t.splitLeafIntoTowleaves(leaf, new_leaf); err != nil {
		return err
	}
	//将改动持久化到磁盘中
	if err = t.flushNodesAndPutNodesPool(new_leaf, leaf); err != nil {
		return err
	}

	// insert split key into parent
	// 只要有分裂操作出现就需要改动父节点的信息
	return t.insertIntoParent(leaf.Parent, leaf.Self, leaf.Keys[len(leaf.Keys)-1], new_leaf.Self)
}

func getIndex(keys []uint64, key uint64) int {
	idx := sort.Search(len(keys), func(i int) bool {
		return key <= keys[i]
	})
	return idx
}

// 将newNode(right_off) 调整至parent的children上，没有对parent持久化
func insertIntoNode(parent *Node, idx int, left_off OFFTYPE, key uint64, right_off OFFTYPE) {
	var (
		i int
	)
	parent.Keys = append(parent.Keys, key)
	for i = len(parent.Keys) - 1; i > idx; i-- {
		parent.Keys[i] = parent.Keys[i-1]
	}
	//将右边子节点最大数值调整到相应位置
	parent.Keys[idx] = key
	//这里如果执行过径向调整，那么就需要将右边这个加入children，可以想想为什么？
	//left_off本身就已经children中，必须保证right_off就在left_off下一个位置，idx就是left_off在parent.Children中的位置
	if idx == len(parent.Children) {
		parent.Children = append(parent.Children, right_off)
		return
	}
	tmpChildren := append([]OFFTYPE{}, parent.Children[idx+1:]...)
	parent.Children = append(append(parent.Children[:idx+1], right_off), tmpChildren...)
}

func (t *BPlusTree) insertIntoNodeAfterSplitting(old_node *Node) error {
	var (
		newNode, child, nextNode *Node
		err                      error
		i, split                 int
	)
	// 这里一定是索引节点？
	if newNode, err = t.newIndexNodeFromDisk(); err != nil {
		return err
	}

	split = cut(order)
	// 生成newNode，并把划分给newNode的children的parent指向调整
	for i = split; i <= order; i++ {
		newNode.Children = append(newNode.Children, old_node.Children[i])
		newNode.Keys = append(newNode.Keys, old_node.Keys[i])

		// update new_node children relation
		if child, err = t.newMappingNodeFromPool(old_node.Children[i]); err != nil {
			return err
		}
		child.Parent = newNode.Self
		if err = t.flushNodesAndPutNodesPool(child); err != nil {
			return err
		}
	}

	newNode.Parent = old_node.Parent

	old_node.Children = old_node.Children[:split]
	old_node.Keys = old_node.Keys[:split]
	// 调整索引层链表指向
	newNode.Next = old_node.Next
	old_node.Next = newNode.Self
	newNode.Prev = old_node.Self
	//调整nextNode的Prev指向
	if newNode.Next != INVALID_OFFSET {
		if nextNode, err = t.newMappingNodeFromPool(newNode.Next); err != nil {
			return err
		}
		nextNode.Prev = newNode.Self
		if err = t.flushNodesAndPutNodesPool(nextNode); err != nil {
			return err
		}
	}

	if err = t.flushNodesAndPutNodesPool(old_node, newNode); err != nil {
		return err
	}
	//只要有分裂出现，都需要调整原始节点的父节点的相关数据
	return t.insertIntoParent(old_node.Parent, old_node.Self, old_node.Keys[len(old_node.Keys)-1], newNode.Self)
}

// 这里key是left_off的keys的最后一个
// 叶子节点分离后调整父节点的逻辑
func (t *BPlusTree) insertIntoParent(parent_off OFFTYPE, left_off OFFTYPE, key uint64, right_off OFFTYPE) error {
	var (
		idx    int
		parent *Node
		err    error
		left   *Node
		right  *Node
	)
	// 如果leaf本身就是当前root
	if parent_off == OFFTYPE(INVALID_OFFSET) {
		if left, err = t.newMappingNodeFromPool(left_off); err != nil {
			return err
		}
		if right, err = t.newMappingNodeFromPool(right_off); err != nil {
			return err
		}
		if err = t.newRootNode(left, right); err != nil {
			return err
		}
		return t.flushNodesAndPutNodesPool(left, right)
	}

	if parent, err = t.newMappingNodeFromPool(parent_off); err != nil {
		return err
	}

	idx = getIndex(parent.Keys, key)
	insertIntoNode(parent, idx, left_off, key, right_off)

	if len(parent.Keys) <= order {
		return t.flushNodesAndPutNodesPool(parent)
	}
	// 如果索引节点也满了
	return t.insertIntoNodeAfterSplitting(parent)
}

func (t *BPlusTree) newRootNode(left *Node, right *Node) error {
	var (
		root *Node
		err  error
	)

	if root, err = t.newIndexNodeFromDisk(); err != nil {
		return err
	}
	root.Keys = append(root.Keys, left.Keys[len(left.Keys)-1])
	root.Keys = append(root.Keys, right.Keys[len(right.Keys)-1])
	root.Children = append(root.Children, left.Self)
	root.Children = append(root.Children, right.Self)
	left.Parent = root.Self
	right.Parent = root.Self

	t.rootOff = root.Self
	return t.flushNodeAndPutNodePool(root)
}

// InsertOrUpdateWhat! c return the newval @bool = true insert, = false update
func (t *BPlusTree) InsertOrUpdateWhat(key uint64, c func(bool, string) string) error {
	var (
		node *Node
		err  error
	)
	if c == nil {
		return fmt.Errorf("empty caller")
	}
	// 当前没有结点
	if t.rootOff == INVALID_OFFSET {
		if node, err = t.newTypeNodeFromDisk(uint8(DATA_PAGE)); err != nil {
			return err
		}
		//初始化根节点为叶子节点
		t.rootOff = node.Self
		node.IsActive = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, c(true, ""))
		node.IsLeaf = true
		return t.flushNodeAndPutNodePool(node)
	}

	if node, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(node, key); err != nil {
		return err
	}

	for i, nkey := range node.Keys {
		if nkey == key {
			node.Records[i] = c(false, node.Records[i])
			return t.flushNodesAndPutNodesPool(node)
		}
	}

	// 没找到准备插入操作
	leaf := node
	var idx int
	//idx key插入位置
	if idx, err = insertKeyValIntoLeaf(leaf, key, c(true, "")); err != nil {
		return err
	}

	// update the last key of parent's if necessary
	// 如果插入的key大于当前b+树的最大数值会发生这种情况，需要沿叶子节点向上更新
	if err = t.mayUpdatedLastParentKey(leaf, idx); err != nil {
		return err
	}

	// insert key/val into leaf
	// 如果叶子节点小于当前b树的阶数
	if len(leaf.Keys) <= order {
		return t.flushNodeAndPutNodePool(leaf)
	}
	var new_leaf *Node
	// 叶子节点已满，执行分裂逻辑
	// split leaf so new leaf node
	if new_leaf, err = t.newDataNodeFromDisk(); err != nil {
		return err
	}
	new_leaf.IsLeaf = true
	if err = t.splitLeafIntoTowleaves(leaf, new_leaf); err != nil {
		return err
	}
	//将改动持久化到磁盘中
	if err = t.flushNodesAndPutNodesPool(new_leaf, leaf); err != nil {
		return err
	}

	// insert split key into parent
	// 只要有分裂操作出现就需要改动父节点的信息
	return t.insertIntoParent(leaf.Parent, leaf.Self, leaf.Keys[len(leaf.Keys)-1], new_leaf.Self)
}
func (t *BPlusTree) InsertOrUpdate(key uint64, val string) error {
	var (
		node *Node
		err  error
	)
	// 当前没有结点
	if t.rootOff == INVALID_OFFSET {
		if node, err = t.newTypeNodeFromDisk(uint8(DATA_PAGE)); err != nil {
			return err
		}
		//初始化根节点为叶子节点
		t.rootOff = node.Self
		node.IsActive = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, val)
		node.IsLeaf = true
		return t.flushNodeAndPutNodePool(node)
	}

	if node, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(node, key); err != nil {
		return err
	}

	for i, nkey := range node.Keys {
		if nkey == key {
			node.Records[i] = val
			return t.flushNodesAndPutNodesPool(node)
		}
	}

	// 没找到准备插入操作
	leaf := node
	var idx int
	//idx key插入位置
	if idx, err = insertKeyValIntoLeaf(leaf, key, val); err != nil {
		return err
	}

	// update the last key of parent's if necessary
	// 如果插入的key大于当前b+树的最大数值会发生这种情况，需要沿叶子节点向上更新
	if err = t.mayUpdatedLastParentKey(leaf, idx); err != nil {
		return err
	}

	// insert key/val into leaf
	// 如果叶子节点小于当前b树的阶数
	if len(leaf.Keys) <= order {
		return t.flushNodeAndPutNodePool(leaf)
	}
	var new_leaf *Node
	// 叶子节点已满，执行分裂逻辑
	// split leaf so new leaf node
	if new_leaf, err = t.newDataNodeFromDisk(); err != nil {
		return err
	}
	new_leaf.IsLeaf = true
	if err = t.splitLeafIntoTowleaves(leaf, new_leaf); err != nil {
		return err
	}
	//将改动持久化到磁盘中
	if err = t.flushNodesAndPutNodesPool(new_leaf, leaf); err != nil {
		return err
	}

	// insert split key into parent
	// 只要有分裂操作出现就需要改动父节点的信息
	return t.insertIntoParent(leaf.Parent, leaf.Self, leaf.Keys[len(leaf.Keys)-1], new_leaf.Self)

}
func (t *BPlusTree) Insert(key uint64, val string) error {
	var (
		err  error
		node *Node
	)
	// 如果没有根节点
	if t.rootOff == INVALID_OFFSET {
		if node, err = t.newTypeNodeFromDisk(uint8(DATA_PAGE)); err != nil {
			return err
		}
		//初始化根节点为叶子节点
		t.rootOff = node.Self
		node.IsActive = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, val)
		node.IsLeaf = true
		return t.flushNodeAndPutNodePool(node)
	}

	return t.insertIntoLeaf(key, val)
}

func (t *BPlusTree) Update(key uint64, val string) error {
	var (
		node *Node
		err  error
	)

	if t.rootOff == INVALID_OFFSET {
		return NotFoundKey
	}

	if node, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(node, key); err != nil {
		return err
	}

	for i, nkey := range node.Keys {
		if nkey == key {
			node.Records[i] = val
			return t.flushNodesAndPutNodesPool(node)
		}
	}
	return NotFoundKey
}

// 可能改变径向上节点的最后一个Key!
func (t *BPlusTree) mayUpdatedLastParentKey(leaf *Node, idx int) error {
	// update the last key of parent's if necessary
	// 当且仅当插入位置在leaf的最后一个位置和当前节点不是根节点
	if idx == len(leaf.Keys)-1 && leaf.Parent != INVALID_OFFSET {
		key := leaf.Keys[len(leaf.Keys)-1]
		updateNodeOff := leaf.Parent
		var (
			updateNode *Node
			node       *Node
		)
		//这里重新加载了一次节点，因为改动不想在leaf身上？
		if node, err = t.newMappingNodeFromPool(leaf.Self); err != nil {
			return err
		}
		*node = *leaf
		defer t.putNodePool(node)

		for updateNodeOff != INVALID_OFFSET && idx == len(node.Keys)-1 {
			if updateNode, err = t.newMappingNodeFromPool(updateNodeOff); err != nil {
				return err
			}
			for i, v := range updateNode.Children {
				if v == node.Self {
					idx = i
					break
				}
			}
			updateNode.Keys[idx] = key
			if err = t.flushNodeAndPutNodePool(updateNode); err != nil {
				return err
			}
			updateNodeOff = updateNode.Parent
			*node = *updateNode
		}
	}
	return nil
}

// 因删除 改动索引结点的children指向
func (t *BPlusTree) deleteKeyFromNode(off OFFTYPE, key uint64) error {
	if off == INVALID_OFFSET {
		return nil
	}
	var (
		node      *Node
		nextNode  *Node
		prevNode  *Node
		newRoot   *Node
		childNode *Node
		idx       int
		err       error
	)
	if node, err = t.newMappingNodeFromPool(off); err != nil {
		return err
	}
	idx = getIndex(node.Keys, key)
	removeKeyFromNode(node, idx)

	// update the last key of parent's if necessary
	// 先执行更新父节点的lastKey
	if idx == len(node.Keys) {
		if err = t.mayUpdatedLastParentKey(node, idx-1); err != nil {
			return err
		}
	}

	// if statisfied len
	if len(node.Keys) >= order/2 {
		return t.flushNodesAndPutNodesPool(node)
	}

	if off == t.rootOff && len(node.Keys) == 1 {
		if newRoot, err = t.newMappingNodeFromPool(node.Children[0]); err != nil {
			return err
		}
		node.IsActive = false
		newRoot.Parent = INVALID_OFFSET
		t.rootOff = newRoot.Self
		return t.flushNodesAndPutNodesPool(node, newRoot)
	}

	if node.Next != INVALID_OFFSET {
		if nextNode, err = t.newMappingNodeFromPool(node.Next); err != nil {
			return err
		}
		// lease from next node
		if len(nextNode.Keys) > order/2 {
			key := nextNode.Keys[0]
			child := nextNode.Children[0]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(nextNode, 0)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(node, idx); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(node, nextNode, childNode)
		}
		// merge nextNode and curNode
		if node.Prev != INVALID_OFFSET {
			if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
				return err
			}
			prevNode.Next = nextNode.Self
			nextNode.Prev = prevNode.Self
			if err = t.flushNodesAndPutNodesPool(prevNode); err != nil {
				return err
			}
		} else {
			nextNode.Prev = INVALID_OFFSET
		}

		nextNode.Keys = append(node.Keys, nextNode.Keys...)
		nextNode.Children = append(node.Children, nextNode.Children...)

		// update child's parent
		for _, v := range node.Children {
			if childNode, err = t.newMappingNodeFromPool(v); err != nil {
				return err
			}
			childNode.Parent = nextNode.Self
			if err = t.flushNodesAndPutNodesPool(childNode); err != nil {
				return err
			}
		}

		node.IsActive = false
		t.putFreeBlocks(node)

		if err = t.flushNodesAndPutNodesPool(node, nextNode); err != nil {
			return err
		}

		// delete parent's key recursively
		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys)-1])
	}

	// come here because node.Next = INVALID_OFFSET
	if node.Prev != INVALID_OFFSET {
		if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevNode.Keys) > order/2 {
			key := prevNode.Keys[len(prevNode.Keys)-1]
			child := prevNode.Children[len(prevNode.Children)-1]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(prevNode, len(prevNode.Keys)-1)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(prevNode, len(prevNode.Keys)-1); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(prevNode, node, childNode)
		}
		// merge prevNode and curNode
		prevNode.Next = INVALID_OFFSET
		prevNode.Keys = append(prevNode.Keys, node.Keys...)
		prevNode.Children = append(prevNode.Children, node.Children...)

		// update child's parent
		for _, v := range node.Children {
			if childNode, err = t.newMappingNodeFromPool(v); err != nil {
				return err
			}
			childNode.Parent = prevNode.Self
			if err = t.flushNodesAndPutNodesPool(childNode); err != nil {
				return err
			}
		}

		node.IsActive = false
		t.putFreeBlocks(node)

		if err = t.flushNodesAndPutNodesPool(node, prevNode); err != nil {
			return err
		}

		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys)-2])
	}
	return nil
}

// 删除叶子节点的逻辑
func (t *BPlusTree) deleteKeyFromLeaf(key uint64) error {
	var (
		leaf     *Node
		prevLeaf *Node
		nextLeaf *Node
		err      error
		idx      int
	)
	if leaf, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(leaf, key); err != nil {
		return err
	}

	idx = getIndex(leaf.Keys, key)
	if idx == len(leaf.Keys) || leaf.Keys[idx] != key {
		t.putNodePool(leaf)
		return fmt.Errorf("not found key:%d", key)

	}
	// 删除key
	removeKeyFromLeaf(leaf, idx)

	// if leaf is root
	// 如果是叶子节点是根节点直接返回
	if leaf.Self == t.rootOff {
		return t.flushNodesAndPutNodesPool(leaf)
	}

	// update the last key of parent's if necessary
	// 如果删除的是leaf的最后一个节点，
	// !BUG-FIX
	if idx == len(leaf.Keys) {
		if err = t.mayUpdatedLastParentKey(leaf, idx-1); err != nil {
			return err
		}
	}

	// if satisfied len
	if len(leaf.Keys) >= order/2 {
		return t.flushNodesAndPutNodesPool(leaf)
	}

	if leaf.Next != INVALID_OFFSET {
		if nextLeaf, err = t.newMappingNodeFromPool(leaf.Next); err != nil {
			return err
		}
		// lease from next leaf
		// 下一个节点有多余的节点,移动一个到本节点
		if len(nextLeaf.Keys) > order/2 {
			key := nextLeaf.Keys[0]
			rec := nextLeaf.Records[0]
			removeKeyFromLeaf(nextLeaf, 0)
			if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(leaf, idx); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(nextLeaf, leaf)
		}
		// 如果下一个节点没有多余的节点，将当前节点和下一个节点合并成一个节点
		// merge nextLeaf and curleaf
		if leaf.Prev != INVALID_OFFSET {
			if prevLeaf, err = t.newMappingNodeFromPool(leaf.Prev); err != nil {
				return err
			}
			prevLeaf.Next = nextLeaf.Self
			nextLeaf.Prev = prevLeaf.Self
			if err = t.flushNodesAndPutNodesPool(prevLeaf); err != nil {
				return err
			}
		} else {
			// 当前节点是首节点,将下一个节点选为首结点
			nextLeaf.Prev = INVALID_OFFSET
		}

		nextLeaf.Keys = append(leaf.Keys, nextLeaf.Keys...)
		nextLeaf.Records = append(leaf.Records, nextLeaf.Records...)

		leaf.IsActive = false
		t.putFreeBlocks(leaf)

		if err = t.flushNodesAndPutNodesPool(leaf, nextLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys)-1])
	}

	// come here because leaf.Next = INVALID_OFFSET
	if leaf.Prev != INVALID_OFFSET {
		if prevLeaf, err = t.newMappingNodeFromPool(leaf.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevLeaf.Keys) > order/2 {
			key := prevLeaf.Keys[len(prevLeaf.Keys)-1]
			rec := prevLeaf.Records[len(prevLeaf.Records)-1]
			removeKeyFromLeaf(prevLeaf, len(prevLeaf.Keys)-1)
			if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(prevLeaf, len(prevLeaf.Keys)-1); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(prevLeaf, leaf)
		}
		// merge prevleaf and curleaf
		prevLeaf.Next = INVALID_OFFSET
		prevLeaf.Keys = append(prevLeaf.Keys, leaf.Keys...)
		prevLeaf.Records = append(prevLeaf.Records, leaf.Records...)

		leaf.IsActive = false
		t.putFreeBlocks(leaf)

		if err = t.flushNodesAndPutNodesPool(leaf, prevLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys)-2])
	}

	return nil
}

func (t *BPlusTree) Delete(key uint64) error {
	if t.rootOff == INVALID_OFFSET {
		return fmt.Errorf("not found key:%d", key)
	}
	return t.deleteKeyFromLeaf(key)
}

func (t *BPlusTree) DebugBPlusTreePrint() error {
	if t.rootOff == INVALID_OFFSET {
		return fmt.Errorf("root = nil")
	}
	Q := make([]OFFTYPE, 0)
	Q = append(Q, t.rootOff)

	floor := 0
	var (
		curNode *Node
		err     error
	)
	for 0 != len(Q) {
		floor++

		l := len(Q)
		fmt.Printf("floor %3d:", floor)
		for i := 0; i < l; i++ {
			if curNode, err = t.newMappingNodeFromPool(Q[i]); err != nil {
				return err
			}
			defer t.putNodePool(curNode)

			// print keys
			if i == l-1 {
				fmt.Printf("%d\n", curNode.Keys)
			} else {
				fmt.Printf("%d, ", curNode.Keys)
			}
			for _, v := range curNode.Children {
				Q = append(Q, v)
			}
		}
		Q = Q[l:]
	}
	return nil
}
