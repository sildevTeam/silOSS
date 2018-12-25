package storage

var Engine *Storage

func init() {
	Engine = NewStorage("/tmp/chunk/1.chunk", "/tmp/index")
	err := Engine.Open()
	if err != nil {
		return
	}
}
