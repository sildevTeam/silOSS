package storage

import (
	"io/ioutil"
	"testing"
)

func TestStorage_Store(t *testing.T) {
	s := NewStorage("/tmp/chunk/1.chunk", "/tmp/index")
	err := s.Open()
	defer s.Close()
	if err != nil {
		t.Fatal(err)
	}

	bts, err := ioutil.ReadFile("testdata/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	err = s.Store("test.txt", bts, FdNullFlags)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStorage_Read(t *testing.T) {
	s := NewStorage("/tmp/chunk/1.chunk", "/tmp/index")
	err := s.Open()
	defer s.Close()
	if err != nil {
		t.Fatal(err)
	}
	i := s.index.slots[1]

	t.Log(i.fId)
	err, name, bts := s.Read(i.fId)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(name)
	t.Log(len(bts))
	t.Log(string(bts))
}
