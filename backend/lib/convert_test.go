package lib

import (
	"fmt"
	"testing"
	"time"
)

func TestMakeMDByByte(t *testing.T) {
	b := []byte("jfjkldhfjksadhfhiofdasdasjasdi")
	s := MakeMDByByte(b)
	fmt.Printf("md:%s,len:%d\n", s, len(s))
}

func TestInt64ToBytes(t *testing.T) {
	i := time.Now().UnixNano()
	b := Int64ToBytes(i)
	fmt.Println(len(b))
}
