package storage

const (
	// File flags
	FdNullFlags  = 0x0
	FdPrivate    = 0x1  // private file flag
	FdDeleted    = 0x2  // logical deleted file flag
	FdExecutable = 0x4  // can execute
	FdIdMd5      = 0x8  // file id calculated by md5
	FdIdCrc32    = 0x10 // file if calculated by crc32
	FdFlag1      = 0x20 // reserved flag 1
	FdFlag2      = 0x40 // reserved flag 2
	FdFlag3      = 0x80 // reserved flag 3
)
