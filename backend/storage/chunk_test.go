package storage

import (
	"io/ioutil"
	"testing"
)

func TestChunk_Open(t *testing.T) {
	c := NewChunk("/tmp/chunk/1.chunk")
	if err := c.Open(); err != nil {
		t.Log(err)
	}
	t.Logf("%#v", c)
}

func TestChunk_AppendBlock(t *testing.T) {
	c := NewChunk("/tmp/chunk/1.chunk")
	if err := c.Open(); err != nil {
		t.Log(err)
	}
	t.Logf("%#v", c)

	if bts, e := ioutil.ReadFile("testdata/test.txt"); e == nil {
		block := NewBlock()
		block.SetBlock("test.txt", FdNullFlags, &bts)
		if err, i := c.AppendBlock(block); err == nil {
			t.Logf("%#v", i)
			t.Logf("%#v", c)
		} else {
			t.Fatal(err)
		}

	} else {
		t.Fatal(e)
	}

}

func BenchmarkChunk_AppendBlock(b *testing.B) {
	c := NewChunk("/tmp/chunk/1.chunk")
	if err := c.Open(); err != nil {
		b.Log(err)
	}

	if bts, e := ioutil.ReadFile("testdata/test.txt"); e == nil {
		block := NewBlock()
		block.SetBlock("test.txt", FdNullFlags, &bts)
		for i := 0; i < b.N; i++ {
			if err, _ := c.AppendBlock(block); err == nil {
			} else {
				b.Fatal(err)
			}
		}
	} else {
		b.Fatal(e)
	}
}
