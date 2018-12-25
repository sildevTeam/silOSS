package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

const (
	blockHeaderSz = 0 +
		1 + 4 +
		8 + 1 +
		8 + 8 +
		0
)

type Block struct {
	flags     int8
	crc32     uint32
	timestamp int64
	fNameSz   uint8
	fileName  string
	fSz       int64
	fOffset   int64
	file      []byte
	block     []byte
	r         *io.Reader
}

func NewBlock() *Block {
	b := new(Block)
	return b
}

// set block para for file write
func (b *Block) SetBlock(name string, flags int8, f *[]byte) {

	b.flags = flags
	b.crc32 = crc32.ChecksumIEEE(*f)
	b.timestamp = time.Now().Unix()
	b.fNameSz = uint8(len([]byte(name)))
	b.fileName = name
	b.fSz = int64(len(*f))
	b.fOffset = int64(len(*f))
	b.file = *f
}

func (b *Block) OnDiskSize() int64 {
	return blockHeaderSz + int64(len(b.file))
}

func (b *Block) WriteTo(w io.Writer) (n int64, err error) {
	// append to file
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, b.crc32); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, b.flags); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, b.timestamp); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, b.fNameSz); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, []byte(b.fileName)); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, b.fSz); err != nil {
		return 0, err
	}
	if err := binary.Write(&buf, binary.BigEndian, b.fOffset); err != nil {
		return 0, err
	}
	buf.Write(b.file)

	return buf.WriteTo(w)
}

func ReadBlock(r io.Reader) (error, *Block) {
	b := NewBlock()
	if err := binary.Read(r, binary.BigEndian, &b.crc32); err != nil {
		return err, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.flags); err != nil {
		return err, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.timestamp); err != nil {
		return err, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.fNameSz); err != nil {
		return err, nil
	}

	fNameBts := make([]byte, b.fNameSz)
	if _, err := io.ReadFull(r, fNameBts); err == nil {
		b.fileName = string(fNameBts)
	} else {
		return err, nil
	}

	if err := binary.Read(r, binary.BigEndian, &b.fSz); err != nil {
		return err, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.fOffset); err != nil {
		return err, nil
	}

	b.block = make([]byte, b.fSz)
	_, err := io.ReadFull(r, b.block)
	if err != nil {
		return err, nil
	}

	return nil, b
}

func transferBlock(r io.Reader) (error, *Block, *io.Reader) {
	b := NewBlock()
	if err := binary.Read(r, binary.BigEndian, &b.crc32); err != nil {
		return err, nil, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.flags); err != nil {
		return err, nil, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.timestamp); err != nil {
		return err, nil, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.fNameSz); err != nil {
		return err, nil, nil
	}

	fNameBts := make([]byte, b.fNameSz)
	if _, err := io.ReadFull(r, fNameBts); err == nil {
		b.fileName = string(fNameBts)
	} else {
		return err, nil, nil
	}

	if err := binary.Read(r, binary.BigEndian, &b.fSz); err != nil {
		return err, nil, nil
	}
	if err := binary.Read(r, binary.BigEndian, &b.fOffset); err != nil {
		return err, nil, nil
	}

	reader := io.LimitReader(r, b.fSz)
	return nil, b, &reader
}
