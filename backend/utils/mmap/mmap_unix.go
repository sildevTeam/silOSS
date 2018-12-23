// +build darwin dragonfly freebsd linux nacl netbsd openbsd

package mmap

import (
	"os"
	"syscall"
)

func ReadOnlyMap(path string, sz int64) ([]byte, error) {
	return mmap(path, sz, syscall.PROT_READ, syscall.MAP_SHARED, true)
}

func RWMap(path string, sz int64) ([]byte, error) {
	return mmap(path, sz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED, false)
}

func UnMap(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.Munmap(data)
}

func mmap(path string, sz int64, port int, flag int, readOnly bool) ([]byte, error) {
	var f *os.File
	var err error
	if readOnly {
		f, err = os.Open(path)
	} else {
		f, err = os.OpenFile(path, syscall.O_RDWR, 0644)
	}

	if err != nil {
		return nil, err
	}
	defer f.Close()

	fStat, err := f.Stat()
	if err != nil {
		return nil, err
	} else if fStat.Size() == 0 {
		return nil, nil
	}

	if sz == 0 {
		sz = fStat.Size()
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(sz), port, flag)
	if err != nil {
		return nil, err
	}

	return data, nil
}
