package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

const (
	BLOCK_HEADER_SZ = 0 +
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
	b.file = *f
}

func (b *Block) OnDiskSize() int64 {
	return BLOCK_HEADER_SZ + int64(len(b.file))
}

// write appends new block to the end of chunk file,where marked as 'maxOffset' in header
// if chunk file's max size is reached ,then generate a new chunk file and write the block in.
// writer should be single instance and prevent
func (b *Block) WriteTo(w io.Writer) (n int64, err error) {
	// append to file
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, b.crc32)
	binary.Write(&buf, binary.BigEndian, b.flags)
	binary.Write(&buf, binary.BigEndian, b.timestamp)
	binary.Write(&buf, binary.BigEndian, b.fNameSz)
	binary.Write(&buf, binary.BigEndian, b.fileName)
	binary.Write(&buf, binary.BigEndian, b.fSz)
	binary.Write(&buf, binary.BigEndian, b.fOffset)
	buf.Write(b.file)

	return buf.WriteTo(w)
}
