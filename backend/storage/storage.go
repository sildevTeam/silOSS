package storage

import (
	"errors"
	"fmt"
	"io"
)

type Storage struct {
	index     *Index
	currChunk *Chunk
	chunkList []Chunk
	// chunk map of chunk name and chunk instance
	chunkMap map[uint32]*Chunk
}

func NewStorage(pChunk string, pIndex string) *Storage {
	s := new(Storage)
	s.index = NewIndex(pIndex)
	s.currChunk = NewChunk(pChunk)
	s.chunkList = make([]Chunk, 0)
	s.chunkList = append(s.chunkList, *s.currChunk)
	return s
}

func (s *Storage) Open() error {
	if err := s.index.Open(); err != nil {
		return err
	}
	if err := s.currChunk.Open(); err != nil {
		return err
	}
	if unit, err := s.currChunk.GetChunkUint(); err == nil {
		s.chunkMap = make(map[uint32]*Chunk)
		s.chunkMap[unit] = s.currChunk
	} else {
		return err
	}
	return nil
}

func (s *Storage) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.currChunk.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Storage) Store(name string, bts []byte, flags int8) error {
	b := NewBlock()
	b.SetBlock(name, flags, &bts)
	// if already has one

	if find := s.index.FindByMerkle(b.crc32); find {
		return errors.New("already has one")
	}

	s.decideChunk()
	err, slot := s.currChunk.AppendBlock(b)
	if err != nil {
		return err
	}
	err = s.index.Insert(*slot)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) decideChunk() {
	if s.currChunk.maxOffset >= defaultSegmentSize {
		lastUnit, _ := s.currChunk.GetChunkUint()
		unit := lastUnit + 1
		path := getChunkPath(unit)
		c := NewChunk(path)
		err := c.Open()
		if err != nil {
			return
		}
		s.currChunk = c
		s.chunkMap[unit] = c
	}
}

func (s *Storage) Read(crc32 uint32) (err error, name string, f []byte) {
	if find, slot := s.index.find(crc32); find == true {
		//read from chunk file
		if u, err := s.currChunk.GetChunkUint(); err == nil && u == slot.chunkFile {
			// file in curr chunk file
			_, b := s.currChunk.ReadBlock(slot.offset)
			return nil, b.fileName, b.block
		} else if s.chunkMap[slot.chunkFile] != nil {
			// open mapped chunk
			_, b := s.chunkMap[slot.chunkFile].ReadBlock(slot.offset)
			return nil, b.fileName, b.block
		} else {
			// open chunk file
			chunk := NewChunk(getChunkPath(slot.chunkFile))
			if err := chunk.Open(); err != nil {
				return err, "", nil
			}
			// append to opened chunk map first
			s.chunkMap[slot.chunkFile] = chunk
			_, b := chunk.ReadBlock(slot.offset)
			return nil, b.fileName, b.block
		}
	} else {
		return errors.New("file not find in index"), "", nil
	}
}

func (s *Storage) Transfer(crc32 uint32) (err error, name string, sz int64, r *io.Reader) {
	if find, slot := s.index.find(crc32); find == true {
		//read from chunk file
		if u, err := s.currChunk.GetChunkUint(); err == nil && u == slot.chunkFile {
			// file in curr chunk file
			e, b, r := s.currChunk.TransferBlock(slot.offset)
			return e, b.fileName, b.fSz, r
		} else if s.chunkMap[slot.chunkFile] != nil {
			// open mapped chunk
			e, b, r := s.chunkMap[slot.chunkFile].TransferBlock(slot.offset)
			return e, b.fileName, b.fSz, r
		} else {
			// open chunk file
			chunk := NewChunk(getChunkPath(slot.chunkFile))
			if err := chunk.Open(); err != nil {
				return err, "", 0, nil
			}
			// append to opened chunk map first
			s.chunkMap[slot.chunkFile] = chunk
			e, b, r := chunk.TransferBlock(slot.offset)
			return e, b.fileName, b.fSz, r
		}
	} else {
		return errors.New("file not find in index"), "", 0, nil
	}
}

func getChunkPath(u uint32) string {
	return "/tmp/chunk/" + fmt.Sprint(u) + chunkFileSuffix
}
