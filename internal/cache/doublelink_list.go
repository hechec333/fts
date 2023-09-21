package cache

import (
	"fmt"
	"math/rand"
	"time"
)

var headNumber = 0
var tailNumber = 0

func init() {
	rand.Seed(time.Now().Unix())
	headNumber = rand.Int()
	tailNumber = rand.Int()
}

type LinkedListNode struct {
	Prev  *LinkedListNode
	Next  *LinkedListNode
	Value interface{}
}

type DoubleLinkedList struct {
	len  int64
	Head *LinkedListNode
	Tail *LinkedListNode
}

type LinkedListIterator struct {
	S *DoubleLinkedList
	P *LinkedListNode
}

type List = DoubleLinkedList
type ListIter = LinkedListIterator

func newListNode(v interface{}) *LinkedListNode {
	return &LinkedListNode{
		Prev:  nil,
		Next:  nil,
		Value: v,
	}
}
func NewList() *List {
	list := new(DoubleLinkedList)
	list.Head = newListNode(headNumber)
	list.Tail = newListNode(tailNumber)
	list.Head.Prev = nil
	list.Tail.Next = nil
	list.Head.Next = list.Tail
	list.Tail.Prev = list.Head
	list.len = 0
	return list
}
func (d *List) Len() int64 {
	return d.len
}
func (d *List) Append(index int64, value interface{}) *LinkedListNode {
	if index > d.len {
		panic(fmt.Sprintf("out of index,current:%v,limit:%v", index, d.len))
	}
	node := newListNode(value)
	var ptr *LinkedListNode = d.Head
	for i := int64(0); i < index && ptr != nil; i++ {
		ptr = ptr.Next
	}
	node.Prev = ptr
	node.Next = ptr.Next
	ptr.Next.Prev = node
	ptr.Next = node
	d.len++
	return node
}

func (d *List) PushBack(value interface{}) *LinkedListNode {
	d.len++
	node := newListNode(value)
	ptr := d.Tail.Prev
	node.Next = d.Tail
	node.Prev = ptr
	d.Tail.Prev = node
	ptr.Next = node
	return node
}


func (d *List) PushFront(value interface{}) *LinkedListNode {
	d.len++
	node := newListNode(value)
	node.Prev = d.Head
	node.Next = d.Head.Next
	d.Head.Next = node
	node.Next.Prev = node

	return node
}

func (d *List) HeadIter() ListIter {
	return LinkedListIterator{
		S: d,
		P: d.Head.Next,
	}
}
func (d *List) TailIter() ListIter {
	return LinkedListIterator{
		S: d,
		P: d.Tail.Prev,
	}
}

func (i *LinkedListIterator) Prev() {
	if i.P == nil {
		panic("Prev() out of range,null pointer ref")
	}
	i.P = i.P.Prev
}

func (i *LinkedListIterator) Next() {
	if i.P == nil {
		panic("Next() out of range,null pointer ref")
	}
	i.P = i.P.Next
}

func (i *LinkedListIterator) Value() interface{} {
	if i.P == i.S.Head || i.P == i.S.Tail {
		panic("Value() out of range,Head or Tail Node ref")
	}
	return i.P.Value
}

func (i *LinkedListIterator) Delete() {
	if i.P == i.S.Head || i.P == i.S.Tail {
		panic("Value() out of range,Head or Tail Node ref")
	}
	prev := i.P.Prev
	prev.Next = i.P.Next
	i.P.Next.Prev = prev
	i.P.Next = nil
	i.P.Prev = nil
	i.S.len--
	i.S = nil
}
