package composer

import (
	"silOSS/backend/storage"
	"testing"
)

func TestNewDirt(t *testing.T) {
	idx := storage.NewIndex("/tmp/index")
	idx.Open()
	defer idx.Close()

	dirt := NewDirt()

	for _, v := range idx.GetSlots() {
		dirt.add(v)
	}
	t.Logf("%#v", dirt)
}
