package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"silOSS/backend/utils/mmap"
	"sync"
)

const (
	IndexVersion    = 1
	IndexMagic      = "SILOSS"
	IndexHeaderSize = 0 +
		6 + 1 + // magic &version
		8 + // max offset
		8 + // total size
		0
	IndexSlotSize = 0 +
		4 + // fId
		4 + // chunkFile
		8 + // offset
		0
)

type Index struct {
	path      string // path of index file
	maxOffset int64  // max offset in file
	data      []byte
	slotBytes []byte
	slots     []IndexSlot
	//size      int64
	w *os.File
	l int64
	v uint8
	sync.RWMutex
}

type Slot interface {
}

// 128bit or 16 byte
type IndexSlot struct {
	fId       uint32
	chunkFile uint32
	offset    int64
}

type IndexHeader struct {
	Version   uint8
	MaxOffset int64
	Len       int64
}

// NewIndex returns a new instance of Index representing the index file of given path
func NewIndex(path string) *Index {
	idx := new(Index)
	idx.path = path
	return idx
}

func (idx *Index) Close() (err error) {
	if idx.data != nil {
		return mmap.UnMap(idx.data)
	}
	return nil
}

func (idx *Index) Open() error {
	var err error

	f, err := os.OpenFile(idx.path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if err := func() error {

		if _, err := os.Stat(idx.path); err != nil && !os.IsNotExist(err) {
			return err
		} else if err == nil {
			// map data
			if idx.data, err = mmap.RWMap(idx.path, 0); err != nil {
				return err
			}
			// read header

			idh, err := ReadIndexHeader(idx.data)
			if err != nil {
				return err
			}

			idx.maxOffset = idh.MaxOffset
			idx.l = idh.Len
			idx.v = idh.Version
			idx.w = f

			// read data
			switch idx.v {
			case IndexVersion:
				s, err := readIndexSlotsV1(idx)
				if err != nil {
					return err
				}
				idx.slots = s
				break
			default:
				return errors.New("index file version not match")
				break
			}
		}

		return nil

	}(); err != nil {
		idx.Close()
		return err
	}

	return nil
}

func ReadIndexHeader(data []byte) (h IndexHeader, err error) {
	r := bytes.NewReader(data)
	magic := make([]byte, len(IndexMagic))

	// magic
	if _, err := io.ReadFull(r, magic); err != nil {
		return h, err
	} else if !bytes.Equal([]byte(IndexMagic), magic) {
		return h, errors.New("invalid index file")
	}

	//version
	if err := binary.Read(r, binary.BigEndian, &h.Version); err != nil {
		return h, err
	}

	// max offset
	if err := binary.Read(r, binary.BigEndian, &h.MaxOffset); err != nil {
		return h, err
	}

	// len
	if err := binary.Read(r, binary.BigEndian, &h.Len); err != nil {
		return h, err
	}

	return h, nil
}

// write header
func (h *IndexHeader) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	buf.WriteString(IndexMagic)
	binary.Write(&buf, binary.BigEndian, h.Version)
	binary.Write(&buf, binary.BigEndian, h.MaxOffset)
	binary.Write(&buf, binary.BigEndian, h.Len)
	return buf.WriteTo(w)
}

// length/count of index file
func (idx *Index) OnDiskCount() int64 {
	return idx.l
}

// length/count of in memory index
func (idx *Index) InMemoryCount() int64 {
	return int64(len(idx.slots))
}

func (idx *Index) Insert(slot IndexSlot) (err error) {
	idx.Lock()
	defer idx.Unlock()
	return idx.insert(slot)
}

func (idx *Index) insert(slot IndexSlot) (err error) {
	idx.slots = append(idx.slots, slot)
	idx.updateIndexData(slot)
	return nil
}

// rebuild compact the index file
func (idx *Index) Rebuild() (err error) {
	panic("not implemented")
}

func readIndexSlotsV1(idx *Index) (slots []IndexSlot, err error) {
	idx.Lock()
	defer idx.Unlock()
	data := idx.data
	r := bytes.NewReader(data)
	// skip header
	sz := len(data) - IndexHeaderSize
	sr := io.NewSectionReader(r, IndexHeaderSize, int64(sz))
	if sr.Size()%IndexSlotSize != 0 {
		return slots, errors.New("invalid index size")
	}
	slots = make([]IndexSlot, sr.Size()/IndexSlotSize)
	sl := len(slots)
	for i := 0; i < sl; i++ {
		s := new(IndexSlot)
		binary.Read(sr, binary.BigEndian, &s.fId)
		binary.Read(sr, binary.BigEndian, &s.chunkFile)
		binary.Read(sr, binary.BigEndian, &s.offset)
		slots[i] = *s
	}

	return slots, nil
}

// append new slot to data
func (idx *Index) updateIndexData(s IndexSlot) (err error) {
	var buf bytes.Buffer
	var wBuf bytes.Buffer
	// write slot to []byte
	binary.Write(&buf, binary.BigEndian, s.fId)
	binary.Write(&buf, binary.BigEndian, s.chunkFile)
	binary.Write(&buf, binary.BigEndian, s.offset)
	// append slot bytes to file bytes
	b := buf.Bytes()
	idx.data = append(idx.data, b...)
	// read and update index
	h, err := ReadIndexHeader(idx.data)
	if err != nil {
		return nil
	}
	h.Len++
	h.MaxOffset += IndexSlotSize

	binary.Write(&wBuf, binary.BigEndian, h.MaxOffset)
	binary.Write(&wBuf, binary.BigEndian, h.Len)
	wbs := wBuf.Bytes()

	// update in memory data
	for i := 6; i < IndexHeaderSize-1; i++ {
		fmt.Printf("idx.data[%d],wbs[%d]\n", i, i-6)
		idx.data[i] = wbs[i-6]
	}

	idx.w.Seek(7, 0)
	idx.w.Write(wbs)

	idx.w.Seek(0, 2)
	idx.w.Write(b)

	return nil
}

func NewIndexHeader() (h *IndexHeader) {
	h = new(IndexHeader)
	h.Version = IndexVersion
	h.MaxOffset = 0
	h.Len = 0
	return
}
