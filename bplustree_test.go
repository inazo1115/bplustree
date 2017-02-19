package bplustree

import (
	"reflect"
	"testing"
)

type Int int

type MyItem struct {
	id   Int
	name string
}

func (s MyItem) GetKey() ItemKey {
	return s.id
}

func (a Int) Less(b ItemKey) bool {
	return a < b.(Int)
}

func TestBPlusTree(t *testing.T) {

	tree := NewBPlusTree(3)

	// Insert 0-31 randomly.
	tree.ReplaceOrInsert(MyItem{2, "foo"})
	tree.ReplaceOrInsert(MyItem{21, "foo"})
	tree.ReplaceOrInsert(MyItem{3, "foo"})
	tree.ReplaceOrInsert(MyItem{27, "foo"})
	tree.ReplaceOrInsert(MyItem{13, "foo"})
	tree.ReplaceOrInsert(MyItem{15, "foo"})
	tree.ReplaceOrInsert(MyItem{8, "foo"})
	tree.ReplaceOrInsert(MyItem{5, "foo"})
	tree.ReplaceOrInsert(MyItem{17, "foo"})
	tree.ReplaceOrInsert(MyItem{9, "foo"})
	tree.ReplaceOrInsert(MyItem{30, "foo"})
	tree.ReplaceOrInsert(MyItem{19, "foo"})
	tree.ReplaceOrInsert(MyItem{23, "foo"})
	tree.ReplaceOrInsert(MyItem{31, "foo"})
	tree.ReplaceOrInsert(MyItem{12, "foo"})
	tree.ReplaceOrInsert(MyItem{10, "foo"})
	tree.ReplaceOrInsert(MyItem{22, "foo"})
	tree.ReplaceOrInsert(MyItem{25, "foo"})
	tree.ReplaceOrInsert(MyItem{16, "foo"})
	tree.ReplaceOrInsert(MyItem{0, "foo"})
	tree.ReplaceOrInsert(MyItem{7, "foo"})
	tree.ReplaceOrInsert(MyItem{29, "foo"})
	tree.ReplaceOrInsert(MyItem{14, "foo"})
	tree.ReplaceOrInsert(MyItem{18, "foo"})
	tree.ReplaceOrInsert(MyItem{6, "foo"})
	tree.ReplaceOrInsert(MyItem{24, "foo"})
	tree.ReplaceOrInsert(MyItem{20, "foo"})
	tree.ReplaceOrInsert(MyItem{1, "foo"})
	tree.ReplaceOrInsert(MyItem{11, "foo"})
	tree.ReplaceOrInsert(MyItem{4, "foo"})
	tree.ReplaceOrInsert(MyItem{26, "foo"})
	tree.ReplaceOrInsert(MyItem{28, "foo"})

	tree.Dump()
	// ++ BPlusTree ++
	// degree: 3  length: 32
	// BLOCK:[13]:0xc420096280
	//     BLOCK:[2 5 10]:0xc4200960a0
	//         LEAF:[0 1 2]:0xc420096000
	//             [{0 foo} {1 foo} {2 foo}]
	//             next:0xc420096320
	//         LEAF:[3 4 5]:0xc420096320
	//             [{3 foo} {4 foo} {5 foo}]
	//             next:0xc4200960f0
	//         LEAF:[6 7 8 9 10]:0xc4200960f0
	//             [{6 foo} {7 foo} {8 foo} {9 foo} {10 foo}]
	//             next:0xc4200961e0
	//         LEAF:[11 12 13]:0xc4200961e0
	//             [{11 foo} {12 foo} {13 foo}]
	//             next:0xc420096050
	//     BLOCK:[17 21 24 27]:0xc420096230
	//         LEAF:[14 15 16 17]:0xc420096050
	//             [{14 foo} {15 foo} {16 foo} {17 foo}]
	//             next:0xc4200962d0
	//         LEAF:[18 19 20 21]:0xc4200962d0
	//             [{18 foo} {19 foo} {20 foo} {21 foo}]
	//             next:0xc420096140
	//         LEAF:[22 23 24]:0xc420096140
	//             [{22 foo} {23 foo} {24 foo}]
	//             next:0xc420096370
	//         LEAF:[25 26 27]:0xc420096370
	//             [{25 foo} {26 foo} {27 foo}]
	//             next:0xc420096190
	//         LEAF:[28 29 30 31]:0xc420096190
	//             [{28 foo} {29 foo} {30 foo} {31 foo}]
	//             next:0x0

	if !reflect.DeepEqual(tree.Get(Int(22)), MyItem{Int(22), "foo"}) {
		t.Errorf("error Get")
	}
	if !reflect.DeepEqual(tree.Max(), MyItem{Int(31), "foo"}) {
		t.Errorf("error Max")
	}
	if !reflect.DeepEqual(tree.Min(), MyItem{Int(0), "foo"}) {
		t.Errorf("error Min")
	}
	if tree.Len() != 32 {
		t.Errorf("error Len")
	}
	if !tree.Has(Int(28)) {
		t.Errorf("error Has")
	}

	if out := tree.Delete(Int(10)); !reflect.DeepEqual(out, MyItem{Int(10), "foo"}) {
		t.Errorf("error Delete")
	}

	buf0 := make([]MyItem, 0)
	for scanIt := tree.Scan(); scanIt.HasNext(); {
		item := scanIt.Next()
		buf0 = append(buf0, item.(MyItem))
	}
	if len(buf0) != 31 {
		t.Errorf("error Scan")
	}

	buf1 := make([]MyItem, 0)
	for scanIt := tree.Range(Int(15), Int(20)); scanIt.HasNext(); {
		item := scanIt.Next()
		buf1 = append(buf1, item.(MyItem))
	}
	if len(buf1) != 5 {
		t.Errorf("error Range")
	}
}
