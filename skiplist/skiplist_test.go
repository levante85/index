package skiplist

import (
	"bytes"
	"fmt"
	"testing"
)

func TestInsert(t *testing.T) {
	sk := New()
	if ok := sk.Insert([]byte("carlo")); !ok {
		t.Fatal("Failed to insert New value")
	}

	ok := sk.Find([]byte("carlo"))
	if !ok {
		t.Fatal("Value inserted not found")
	}
}

func TestInsertFind(t *testing.T) {
	sk := New()
	if ok := sk.Insert([]byte("carlo")); !ok {
		t.Fatal("Failed to insert New value")
	}
	if sk.Size() != 1 {
		t.Fatal("Insertion failed")
	}
	if ok := sk.Find([]byte(("carlo"))); !ok {
		t.Fatal("Failed to find value")
	}
}

func TestInsertFindMulti(t *testing.T) {
	sk := New()
	values := []string{
		"carlo1",
		"carlo2",
		"carlo3",
		"carlo4",
		"carlo5",
		"carlo6",
	}
	for i := range values {
		if ok := sk.Insert([]byte(values[i])); !ok {
			t.Fatal("Failed to insert New value")
		}
	}
	if sk.Size() != 6 {
		t.Fatal("Insertion failed")
	}

	for i := range values {
		if ok := sk.Find([]byte(values[i])); !ok {
			t.Fatal("Failed to find value")
		}
	}
}

func TestInsertFindRemove(t *testing.T) {
	sk := New()
	if ok := sk.Insert([]byte("carlo")); !ok {
		t.Fatal("Failed to insert New value")
	}
	if sk.Size() != 1 {
		t.Fatal("Insertion failed")
	}
	if ok := sk.Find([]byte("carlo")); !ok {
		t.Fatal("Failed to find value")
	}
	if ok := sk.Remove([]byte("carlo")); !ok {
		t.Fatal("Failed to remove value")
	}
}

/*
func TestInsertFindRemoveMulti(t *testing.T) {
	sk := New()
	values := []string{
		"carlo1",
		"carlo2",
		"carlo3",
		"carlo4",
		"carlo5",
		"carlo6",
	}
	for i := range values {
		if ok := sk.Insert([]byte(values[i])); !ok {
			t.Fatal("Failed to insert New value")
		}
	}
	if sk.Size() != 6 {
		t.Fatal("Insertion failed")
	}

	for i := range values {
		t.Log("value: ", values[i])
		if ok := sk.Remove([]byte(values[i])); !ok {
			t.Fatal("Failed to remove value")
		}
	}
}
*/

func TestRangeFind(t *testing.T) {
	sk := New()
	values := []string{
		"carlo1",
		"carlo2",
		"carlo3",
		"carlo4",
		"carlo5",
		"carlo6",
	}
	for i := range values {
		if ok := sk.Insert([]byte(values[i])); !ok {
			t.Fatal("Failed to insert New value")
		}
	}

	ok, found := sk.RangeFind([]byte("carlo1"), []byte("carlo4"))
	if !ok && len(found) == 0 {
		t.Fatal("Range not found, should not happend")
	}

	for i := range found {
		if bytes.Equal(found[i], []byte(values[i])) {
		} else {
			t.Fatalf("Should not be here %v and %v\n", string(found[i]), values[i])
		}
	}

}

func TestRangeFindLarge(t *testing.T) {
	sk := New()
	for i := 0; i < 1024*4; i++ {
		sk.Insert([]byte(fmt.Sprintf("%v", i)))
	}

	start, end := "1000", "4000"
	ok, found := sk.RangeFind([]byte(start), []byte(end))
	if !ok {
		t.Fatal("Range find failed", len(found))
	}
}

func BenchmarkPickHeightFast(b *testing.B) {
	sk := New()
	r := make([]int, b.N)
	for n := 0; n < b.N; n++ {
		r = append(r, sk.pickHeight())
	}
}

func BenchmarkInsertion(b *testing.B) {
	sk := New()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1024*24; i++ {
			sk.Insert([]byte(fmt.Sprintf("%v", i)))
		}
	}
	b.Log("Node in use: ", sk.arena.current)
}

func BenchmarkInsertion50(b *testing.B) {
	sk := New()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1024*128; i++ {
			sk.Insert([]byte(fmt.Sprintf("%v", i)))
		}
	}
}

func BenchmarkRangeFind(b *testing.B) {
	sk := New()
	for i := 0; i < 1024*4; i++ {
		sk.Insert([]byte(fmt.Sprintf("%v", i)))
	}

	start, end := []byte("1000"), []byte("4000")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, f := sk.RangeFind(start, end)
		if !ok {
			b.Fatal("Failed range find with size: ", len(f))
		}
	}
}
