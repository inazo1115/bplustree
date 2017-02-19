package bplustree

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

//
// Item and ItemKey
//

type Item interface {
	GetKey() ItemKey
}

type ItemKey interface {
	Less(than ItemKey) bool
}

//
// itemKeys, children and items
//

var (
	nilItemKeys = make(itemKeys, 16)
	nilChildren = make(children, 16)
	nilItems    = make(items, 16)
)

type itemKeys []ItemKey

func (s *itemKeys) insertAt(index int, itemkey ItemKey) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = itemkey
}

func (s *itemKeys) removeAt(index int) ItemKey {
	itemkey := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return itemkey
}

func (s *itemKeys) pop() (out ItemKey) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

func (s *itemKeys) truncate(index int) {
	var toClear itemKeys
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilItemKeys):]
	}
}

func (s itemKeys) find(itemkey ItemKey) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return itemkey.Less(s[i])
	})
	if i > 0 && !s[i-1].Less(itemkey) {
		return i - 1, true
	}
	return i, false
}

type children []node

func (s *children) insertAt(index int, n node) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

func (s *children) removeAt(index int) node {
	n := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

func (s *children) pop() (out node) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

func (s *children) truncate(index int) {
	var toClear children
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

type items []Item

func (s *items) insertAt(index int, item Item) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

func (s *items) removeAt(index int) Item {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return item
}

func (s *items) pop() (out Item) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

func (s *items) truncate(index int) {
	var toClear items
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilItems):]
	}
}

//
// node(blockNode/leafNode)
//

type node interface {
	ItemKeys() itemKeys
	Get(key ItemKey) Item
	Max() Item
	Min() Item
	Insert(item Item, maxItemKeys int) Item
	Remove(key ItemKey, minItemKeys int) Item
	IsLeaf() bool
	Split(i int) (ItemKey, node)
	Dump(level int)
}

type blockNode struct {
	itemKeys itemKeys
	children children
	mtx      sync.RWMutex
}

type leafNode struct {
	itemKeys itemKeys
	items    items
	next     node
	mtx      sync.RWMutex
}

func newBlockNode() *blockNode {
	return &blockNode{}
}

func newleafNode() *leafNode {
	return &leafNode{}
}

func (n *blockNode) ItemKeys() itemKeys {
	return n.itemKeys
}

func (n *leafNode) ItemKeys() itemKeys {
	return n.itemKeys
}

func (n *blockNode) Get(key ItemKey) Item {
	i, _ := n.itemKeys.find(key)
	return n.children[i].Get(key)
}

func (n *leafNode) Get(key ItemKey) Item {
	i, found := n.itemKeys.find(key)
	if found {
		return n.items[i]
	}
	return nil
}

func (n *blockNode) Max() Item {
	return n.children[len(n.children)-1].Max()
}

func (n *leafNode) Max() Item {
	return n.items[len(n.items)-1]
}

func (n *blockNode) Min() Item {
	return n.children[0].Min()
}

func (n *leafNode) Min() Item {
	return n.items[0]
}

func (n *blockNode) Insert(item Item, maxItemKeys int) Item {
	k0 := item.GetKey()
	i, _ := n.itemKeys.find(k0)
	if n.maybeSplitChild(i, maxItemKeys) {
		k1 := n.itemKeys[i]
		if k1.Less(k0) {
			i++
		}
	}
	return n.children[i].Insert(item, maxItemKeys)
}

func (n *leafNode) Insert(item Item, maxItemKeys int) Item {
	if len(n.itemKeys) > maxItemKeys {
		panic("this leaf node can't have more items")
	}
	k := item.GetKey()
	i, found := n.itemKeys.find(k)
	if found {
		out := n.items[i]
		n.items[i] = item
		return out
	}
	n.itemKeys.insertAt(i, k)
	n.items.insertAt(i, item)
	return nil
}

func (n *blockNode) Remove(key ItemKey, minItemKeys int) Item {
	i, _ := n.itemKeys.find(key)
	out := n.children[i].Remove(key, minItemKeys)
	n.balanceChild(i, minItemKeys)
	return out
}

func (n *leafNode) Remove(key ItemKey, minItemKeys int) Item {
	i, found := n.itemKeys.find(key)
	if found {
		out := n.items[i]
		n.itemKeys.removeAt(i)
		n.items.removeAt(i)
		return out
	}
	return nil
}

func (n *blockNode) IsLeaf() bool {
	return false
}

func (n *leafNode) IsLeaf() bool {
	return true
}

func (n *blockNode) Split(i int) (ItemKey, node) {
	itemKey := n.itemKeys[i]
	second := newBlockNode()
	second.itemKeys = append(second.itemKeys, n.itemKeys[i+1:]...)
	n.itemKeys.truncate(i)
	if len(n.children) > 0 {
		second.children = append(second.children, n.children[i+1:]...)
		n.children.truncate(i + 1)
	}
	return itemKey, second
}

func (n *leafNode) Split(i int) (ItemKey, node) {
	itemKey := n.itemKeys[i]
	second := newleafNode()
	second.itemKeys = append(second.itemKeys, n.itemKeys[i+1:]...)
	n.itemKeys.truncate(i + 1)
	if len(n.items) > 0 {
		second.items = append(second.items, n.items[i+1:]...)
		n.items.truncate(i + 1)
	}
	second.next = n.next
	n.next = second
	return itemKey, second
}

func (n *blockNode) Dump(level int) {
	indent := strings.Repeat("    ", level)
	fmt.Printf("%sBLOCK:%v:%p\n", indent, n.itemKeys, n)
	for _, c := range n.children {
		c.Dump(level + 1)
	}
}

func (n *leafNode) Dump(level int) {
	indent := strings.Repeat("    ", level)
	fmt.Printf("%sLEAF:%v:%p\n", indent, n.itemKeys, n)
	fmt.Printf("%s    %v\n", indent, n.items)
	fmt.Printf("%s    next:%p\n", indent, n.next)
}

func (n *blockNode) maybeSplitChild(i, maxItemKeys int) bool {
	if len(n.children[i].ItemKeys()) < maxItemKeys {
		return false
	}
	first := n.children[i]
	itemKey, second := first.Split(maxItemKeys / 2)
	n.itemKeys.insertAt(i, itemKey)
	n.children.insertAt(i+1, second)
	return true
}

func (n *blockNode) balanceChild(i, minItemKeys int) {
	// Re-balance is unnecesary.
	if len(n.children[i].ItemKeys()) > minItemKeys {
		return
	}
	// Borrow from left sibling.
	if i > 0 && len(n.children[i-1].ItemKeys())-1 >= minItemKeys {
		left := n.children[i-1]
		right := n.children[i]
		var k ItemKey
		if left.IsLeaf() {
			k = borrowFromLeftLeaf(left.(*leafNode), right.(*leafNode))
		} else {
			k = borrowFromLeftBlock(left.(*blockNode), right.(*blockNode))
		}
		n.itemKeys[i-1] = k
		return
	}
	// Borrow from right sibling.
	if i < len(n.itemKeys)-1 && len(n.children[i+1].ItemKeys())-1 >= minItemKeys {
		left := n.children[i]
		right := n.children[i+1]
		var k ItemKey
		if left.IsLeaf() {
			k = borrowFromRightLeaf(left.(*leafNode), right.(*leafNode))
		} else {
			k = borrowFromRightBlock(left.(*blockNode), right.(*blockNode))
		}
		n.itemKeys[i-1] = k
		return
	}
	// Merge left sibling.
	if i > 0 {
		first := n.children[i-1]
		second := n.children[i]
		if first.IsLeaf() {
			mergeLeaf(first.(*leafNode), second.(*leafNode))
		} else {
			mergeBlock(first.(*blockNode), second.(*blockNode))
		}
		n.itemKeys.removeAt(i - 1)
		n.children.removeAt(i)
		return
	}
	// Merge right sibling.
	first := n.children[i]
	second := n.children[i+1]
	if first.IsLeaf() {
		mergeLeaf(first.(*leafNode), second.(*leafNode))
	} else {
		mergeBlock(first.(*blockNode), second.(*blockNode))
	}
	n.itemKeys.removeAt(i)
	n.children.removeAt(i + 1)
	return
}

func mergeBlock(first, second *blockNode) {
	first.itemKeys = append(first.itemKeys, second.itemKeys...)
	first.children = append(first.children, second.children...)
}

func mergeLeaf(first, second *leafNode) {
	first.itemKeys = append(first.itemKeys, second.itemKeys...)
	first.items = append(first.items, second.items...)
}

func borrowFromLeftBlock(left, right *blockNode) ItemKey {
	k := left.itemKeys.pop()
	c := left.children.pop()
	right.itemKeys.insertAt(0, k)
	right.children.insertAt(0, c)
	return left.itemKeys[len(left.itemKeys)-1]
}

func borrowFromLeftLeaf(left, right *leafNode) ItemKey {
	k := left.itemKeys.pop()
	i := left.items.pop()
	right.itemKeys.insertAt(0, k)
	right.items.insertAt(0, i)
	return left.itemKeys[len(left.itemKeys)-1]
}

func borrowFromRightBlock(left, right *blockNode) ItemKey {
	k := right.itemKeys.removeAt(0)
	c := right.children.removeAt(0)
	left.itemKeys.insertAt(len(left.itemKeys), k)
	left.children.insertAt(len(left.children), c)
	return left.itemKeys[len(left.itemKeys)-1]
}

func borrowFromRightLeaf(left, right *leafNode) ItemKey {
	k := right.itemKeys.removeAt(0)
	i := right.items.removeAt(0)
	left.itemKeys.insertAt(len(left.itemKeys), k)
	left.items.insertAt(len(left.items), i)
	return left.itemKeys[len(left.itemKeys)-1]
}

//
// BPlusTree
//

type BPlusTree struct {
	degree int
	length int
	root   node
}

func NewBPlusTree(degree int) *BPlusTree {
	return &BPlusTree{degree, 0, nil}
}

func (t *BPlusTree) maxItemKeys() int {
	return t.degree*2 - 1
}

func (t *BPlusTree) minItemKeys() int {
	return t.degree - 1
}

func (t *BPlusTree) ReplaceOrInsert(item Item) Item {
	if item == nil {
		panic("nil item being added to BPlusTree")
	}
	if t.root == nil {
		t.root = newleafNode()
		t.root.Insert(item, t.maxItemKeys())
		t.length++
		return nil
	}
	if len(t.root.ItemKeys()) >= t.maxItemKeys() {
		itemKey, second := t.root.Split(t.maxItemKeys() / 2)
		oldroot := t.root
		newroot := newBlockNode()
		newroot.itemKeys = append(newroot.itemKeys, itemKey)
		newroot.children = append(newroot.children, oldroot, second)
		t.root = newroot
	}
	out := t.root.Insert(item, t.maxItemKeys())
	if out == nil {
		t.length++
	}
	return out
}

func (t *BPlusTree) Get(key ItemKey) Item {
	if t.root == nil {
		return nil
	}
	return t.root.Get(key)
}

func (t *BPlusTree) Max() Item {
	if t.root == nil {
		return nil
	}
	return t.root.Max()
}

func (t *BPlusTree) Min() Item {
	if t.root == nil {
		return nil
	}
	return t.root.Min()
}

func (t *BPlusTree) Delete(key ItemKey) Item {
	if t.root == nil {
		return nil
	}
	out := t.root.Remove(key, t.minItemKeys())
	if out != nil {
		t.length--
	}
	if len(t.root.ItemKeys()) == 0 {
		if t.root.IsLeaf() {
			t.root = nil
		} else {
			t.root = t.root.(*blockNode).children[0]
		}
	}
	return out
}

func (t *BPlusTree) Len() int {
	return t.length
}

func (t *BPlusTree) Has(key ItemKey) bool {
	return t.Get(key) != nil
}

func (t *BPlusTree) Dump() {
	fmt.Printf("++ BPlusTree ++\n")
	fmt.Printf("degree: %d  length: %d\n", t.degree, t.length)
	if t.root == nil {
		fmt.Printf("the tree is empty.\n")
	} else {
		t.root.Dump(0)
	}
}