package internal

import (
	"fmt"
	"os"
	"testing"
)

func TestBPlusTree(t *testing.T) {
	var (
		BPlusTree *BPlusTree
		err       error
	)
	if BPlusTree, err = NewBPlusTree("./data.db"); err != nil {
		t.Fatal(err)
	}

	// insert
	for i := 0; i < 20; i++ {
		val := fmt.Sprintf("%d", i)
		if err = BPlusTree.Insert(uint64(i), val); err != nil {
			t.Fatal(err)
		}
	}

	// insert same key repeatedly
	for i := 0; i < 20; i++ {
		val := fmt.Sprintf("%d", i)
		if err = BPlusTree.Insert(uint64(i), val); err != HasExistedKeyError {
			t.Fatal(err)
		}
	}

	// find key
	for i := 0; i < 20; i++ {
		oval := fmt.Sprintf("%d", i)
		if val, err := BPlusTree.Find(uint64(i)); err != nil {
			t.Fatal(err)
		} else {
			if oval != val {
				t.Fatal(fmt.Sprintf("not equal key:%d oval:%s, found val:%s", i, oval, val))
			}
		}
	}

	// first print
	BPlusTree.DebugBPlusTreePrint()

	// delete two keys
	if err := BPlusTree.Delete(0); err != nil {
		t.Fatal(err)
	}
	if err := BPlusTree.Delete(2); err != nil {
		t.Fatal(err)
	}

	if _, err := BPlusTree.Find(2); err != NotFoundKey {
		t.Fatal(err)
	}

	// close BPlusTree
	BPlusTree.Close()

	//repoen BPlusTree
	if BPlusTree, err = NewBPlusTree("./data.db"); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("./data.db")
	defer BPlusTree.Close()

	// find
	if _, err := BPlusTree.Find(2); err != NotFoundKey {
		t.Fatal(err)
	}

	// update {key: 18, val : "19"}
	if err := BPlusTree.Update(18, "19"); err != nil {
		t.Fatal(err)
	}

	// find {key: 18, val : "19"}
	if val, err := BPlusTree.Find(18); err != nil {
		t.Fatal(err)
	} else if "19" != val {
		t.Fatal(fmt.Errorf("Expect %s, but get %s", "19", val))
	}

	// second print
	BPlusTree.DebugBPlusTreePrint()
}
