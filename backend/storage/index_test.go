package storage

import (
	"hash/crc32"
	"os"
	"testing"
)

func TestNewIndexHeader(t *testing.T) {
	h := NewIndexHeader()

	f, _ := os.Create("/tmp/index")
	h.WriteTo(f)

}

func TestIndex_Open(t *testing.T) {
	idx := NewIndex("/tmp/index")
	err := idx.Open()
	defer idx.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("slots: %#v", idx.slots)
	t.Logf("count: %#v", idx.l)
	t.Logf("max offset: %#v", idx.maxOffset)
}

func TestIndex_Insert(t *testing.T) {
	idx := NewIndex("/tmp/index")
	err := idx.Open()
	defer idx.Close()
	if err != nil {
		t.Fatal(err)
	}
	s := new(IndexSlot)
	s.fId = crc32.ChecksumIEEE([]byte("hello"))
	s.offset = int64(123)
	s.chunkFile = uint32(1)
	idx.Insert(*s)
}

func TestNewIndex(t *testing.T) {
}
