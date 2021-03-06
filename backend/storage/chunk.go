package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultSegmentSize = 2 * 1024 * 1024 * 1024 //2G
	chunkFileVersion   = 0x1
	chunkMagic         = "SILOSSC"
	chunkHeaderCount   = 0 +
		7 + 1 +
		8 + 8 +
		8 + 8 +
		0
	chunkFileSuffix = ".chunk"
)

type Chunk struct {
	path      string
	fName     string
	sum       int64
	size      int64
	cTime     int64
	maxOffset int64
	blockIds  []uint32

	w *os.File

	sync.RWMutex
}

func NewChunk(p string) *Chunk {
	c := new(Chunk)
	c.path = p

	return c
}

func (c *Chunk) Close() error {
	return c.w.Close()
}

func (c *Chunk) Open() error {
	c.Lock()
	defer c.Unlock()

	if _, err := os.Stat(c.path); err != nil && !os.IsNotExist(err) {
		return err
	} else if _, err := os.Stat(c.path); err != nil && os.IsNotExist(err) {
		// file not exist create a new one and open
		if err := makeDir(c.path); err != nil {
			return err
		}
		f, err := os.OpenFile(c.path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		c.w = f

		// set defaults
		c.sum = 0
		c.size = 0
		c.cTime = time.Now().Unix()
		// offset relative to the start position of the chunk file
		c.maxOffset = chunkHeaderCount

		//write default header
		if err = c.WriteHeader(); err != nil {
			return err
		}
		c.blockIds = make([]uint32, 0)

		// proceed remaining normal open procedure

		c.fName = c.getFName()
	} else if err == nil {
		f, err := os.OpenFile(c.path, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		c.w = f

		// normal open
		err = c.ReadHeader()
		if err != nil {
			return err
		}
		c.fName = c.getFName()

	}

	return nil
}

func (c *Chunk) WriteHeader() error {
	var buf bytes.Buffer
	buf.WriteString(chunkMagic)
	if err := binary.Write(&buf, binary.BigEndian, chunkFileVersion); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, c.sum); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, c.size); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, c.cTime); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, c.maxOffset); err != nil {
		return err
	}
	h := buf.Bytes()
	_, err := c.w.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = c.w.Write(h)
	if err != nil {
		return err
	}
	return nil
}

func (c *Chunk) ReadHeader() error {

	magic := make([]byte, len(chunkMagic))
	// check magic
	if _, err := io.ReadFull(c.w, magic); err != nil {
		return err
	} else if !bytes.Equal(magic, []byte(chunkMagic)) {
		return errors.New("invalid chunk file")
	}
	// sum
	if err := binary.Read(c.w, binary.BigEndian, &c.sum); err != nil {
		return err
	}
	// size
	if err := binary.Read(c.w, binary.BigEndian, &c.size); err != nil {
		return err
	}
	// cTime
	if err := binary.Read(c.w, binary.BigEndian, &c.cTime); err != nil {
		return err
	}
	// maxOffset
	if err := binary.Read(c.w, binary.BigEndian, &c.maxOffset); err != nil {
		return err
	}
	return nil
}

// at this point the block only has a valid bytes which representing the file is holds
// the outer caller should insert the index slot to the new Index file
func (c *Chunk) AppendBlock(b *Block) (err error, slot *IndexSlot) {
	if _, err := c.w.Seek(c.maxOffset, 0); err != nil {
		return err, slot
	}
	if _, err := b.WriteTo(c.w); err != nil {
		return err, slot
	}
	c.blockIds = append(c.blockIds, b.crc32)
	c.sum++
	startOffset := c.maxOffset
	c.maxOffset += b.OnDiskSize()
	c.size += b.OnDiskSize()

	slot = new(IndexSlot)
	if c, err := c.GetChunkUint(); err == nil {
		slot.chunkFile = c
	} else {
		return err, nil
	}
	slot.fId = b.crc32
	slot.offset = startOffset

	// sync to file
	e := c.WriteHeader()
	if e != nil {
		return e, slot
	}
	return nil, slot
}

func (c *Chunk) ReadBlock(offset int64) (error, *Block) {
	// seek to the start pos of the file
	if _, err := c.w.Seek(offset, 0); err != nil {
		return err, nil
	}
	return ReadBlock(c.w)
}

// transfer block transfer the reader to a sendfile syscall
func (c *Chunk) TransferBlock(offset int64) (error, *Block, *io.Reader) {
	// seek to the start pos of the file
	if _, err := c.w.Seek(offset, 0); err != nil {
		return err, nil, nil
	}
	return transferBlock(c.w)
}

func makeDir(path string) error {
	i := strings.LastIndex(path, "/")
	p := path[:i] // from i to end
	_, err := os.Stat(p)
	if err != nil && os.IsNotExist(err) {
		return os.MkdirAll(p, os.ModePerm)
	} else {
		return err
	}
}

func (c *Chunk) getFName() string {
	i := strings.LastIndex(c.path, "/")
	return c.path[i+1:]
}

func (c *Chunk) GetChunkUint() (uint32, error) {
	i := strings.LastIndex(c.getFName(), ".")
	name := c.getFName()[:i]
	if ni, err := strconv.Atoi(name); err == nil {
		return uint32(ni), nil
	} else {
		return 0, err
	}
}
