package storage

const (
	// File flags
	FD_NULL_FLAGS = 0x0
	FD_PRIVATE    = 0x1  // private file flag
	FD_DELETED    = 0x2  // logical deleted file flag
	FD_EXECUTABLE = 0x4  // can execute
	FD_ID_MD5     = 0x8  // file id calculated by md5
	FD_ID_CRC32   = 0x10 // file if calculated by crc32
	FD_FLAG_1     = 0x20 // reserved flag 1
	FD_FLAG_2     = 0x40 // reserved flag 2
	FD_FLAG_3     = 0x80 // reserved flag 3
)
