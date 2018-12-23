package lib

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
)

func MakeMDByByte(initByte []byte) string {
	m := md5.New()
	m.Write(initByte)
	md := m.Sum(nil)
	mdString := hex.EncodeToString(md)
	return mdString
}

func Int64ToBytes(i int64) []byte {
	return []byte(strconv.FormatInt(i, 10))
}
