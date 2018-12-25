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
	indexVersion    = 1
	indexMagic      = "SILOSS"
	indexHeaderSize = 0 +
		6 + 1 + // magic &version
		8 + // max offset
		8 + // total size
		0
	indexSlotSize = 0 +
		4 + // fId
		4 + // chunkFile
		8 + // offset
		0
)

type Index struct {
	// path of index file
	path string
	// max offset in file
	maxOffset int64
	data      []byte
	slotBytes []byte
	slots     []IndexSlot
	w         *os.File
	l         int64
	v         uint8
	sync.RWMutex
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

func (ids *IndexSlot) GetFileId() uint32 {
	return ids.fId
}
func (ids *IndexSlot) GetChunkFile() string {
	return getChunkPath(ids.chunkFile)
}
func (ids *IndexSlot) GetOffset() int64 {
	return ids.offset
}

// NewIndex returns a new instance of Index representing the index file of given path
func NewIndex(path string) *Index {
	idx := new(Index)
	idx.path = path
	return idx
}

func (idx *Index) GetSlots() []IndexSlot {
	return idx.slots
}

func (idx *Index) Close() (err error) {
	if idx.data != nil {
		return mmap.UnMap(idx.data)
	}
	return nil
}

func (idx *Index) Open() error {
	//var err error
	if _, err := os.Stat(idx.path); err != nil && os.IsNotExist(err) {
		if err := makeDir(idx.path); err != nil {
			return err
		}
		f, err := os.OpenFile(idx.path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		} else {
			h := NewIndexHeader()
			if _, err := h.WriteTo(f); err != nil {
				return err
			}
		}
		idx.w = f
	} else if _, err := os.Stat(idx.path); err == nil {
		f, err := os.OpenFile(idx.path, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		idx.w = f
	} else if _, err := os.Stat(idx.path); err != nil && !os.IsNotExist(err) {
		// unknown error occurred
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

			// read data
			switch idx.v {
			case indexVersion:
				s, err := readIndexSlotsV1(idx)
				if err != nil {
					return err
				}
				idx.slots = s
				break
			default:
				return errors.New("index file version not match")
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
	magic := make([]byte, len(indexMagic))

	// magic
	if _, err := io.ReadFull(r, magic); err != nil {
		return h, err
	} else if !bytes.Equal([]byte(indexMagic), magic) {
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
	buf.WriteString(indexMagic)
	if err := binary.Write(&buf, binary.BigEndian, h.Version); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, h.MaxOffset); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, h.Len); err != nil {
		return 0, err
	}
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
	return idx.updateIndexData(slot)
}

// rebuild compact the index file
func (idx *Index) Rebuild() (err error) {
	// todo:compact index file
	panic("not implemented")
}

func readIndexSlotsV1(idx *Index) (slots []IndexSlot, err error) {
	idx.Lock()
	defer idx.Unlock()
	data := idx.data
	r := bytes.NewReader(data)
	// skip header
	sz := len(data) - indexHeaderSize
	sr := io.NewSectionReader(r, indexHeaderSize, int64(sz))
	if sr.Size()%indexSlotSize != 0 {
		return slots, errors.New("invalid index size")
	}
	slots = make([]IndexSlot, sr.Size()/indexSlotSize)
	sl := len(slots)
	for i := 0; i < sl; i++ {
		s := new(IndexSlot)
		if err := binary.Read(sr, binary.BigEndian, &s.fId); err != nil {
			return nil, err
		}
		if err := binary.Read(sr, binary.BigEndian, &s.chunkFile); err != nil {
			return nil, err
		}
		if err := binary.Read(sr, binary.BigEndian, &s.offset); err != nil {
			return nil, err
		}
		slots[i] = *s
	}

	return slots, nil
}

// append new slot to data
func (idx *Index) updateIndexData(s IndexSlot) (err error) {
	var buf bytes.Buffer
	var wBuf bytes.Buffer
	// write slot to []byte
	if err := binary.Write(&buf, binary.BigEndian, s.fId); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, s.chunkFile); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, s.offset); err != nil {
		return err
	}
	// append slot bytes to file bytes
	b := buf.Bytes()
	idx.data = append(idx.data, b...)
	// read and update index
	h, err := ReadIndexHeader(idx.data)
	if err != nil {
		return nil
	}
	h.Len++
	h.MaxOffset += indexSlotSize

	if err := binary.Write(&wBuf, binary.BigEndian, h.MaxOffset); err != nil {
		return err
	}
	if err := binary.Write(&wBuf, binary.BigEndian, h.Len); err != nil {
		return err
	}
	wbs := wBuf.Bytes()

	// update in memory data
	for i := 6; i < indexHeaderSize-1; i++ {
		fmt.Printf("idx.data[%d],wbs[%d]\n", i, i-6)
		idx.data[i] = wbs[i-6]
	}

	if _, err := idx.w.Seek(7, 0); err != nil {
		return err
	}
	if _, err := idx.w.Write(wbs); err != nil {
		return err
	}
	if _, err := idx.w.Seek(0, 2); err != nil {
		return err
	}
	if _, err := idx.w.Write(b); err != nil {
		return err
	}

	return nil
}

func (idx *Index) find(crc32 uint32) (find bool, slot *IndexSlot) {
	for _, v := range idx.slots {
		if v.fId == crc32 {
			return true, &v
		}
	}
	return false, nil
}

func (idx *Index) FindByMerkle(crc32 uint32) (find bool) {
	//todo:write this
	b, _ := idx.find(crc32)
	return b
}

func NewIndexHeader() (h *IndexHeader) {
	h = new(IndexHeader)
	h.Version = indexVersion
	h.MaxOffset = 0
	h.Len = 0
	return
}
