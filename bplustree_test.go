package bplustree

import (
	"math/rand"
	"testing"
)

type Name string

type Int int

type Student struct {
	id   Int
	name string
	age  int
}

func (s Student) GetKey() ItemKey {
	return s.id
}

func (a Int) Less(b ItemKey) bool {
	return a < b.(Int)
}

func TestBPlusTree(t *testing.T) {

	tree := NewBPlusTree(3)

	for i := 0; i < 50; i++ {
		item := Student{Int(rand.Intn(100)), "foo", 0}
		tree.ReplaceOrInsert(item)
	}

	tree.Dump()

	if tree.Len() == 50 {
		t.Errorf("error Len")
	}
}
