package composer

import "silOSS/backend/storage"

type ListNode struct {
	// key hash
	key uint32
	// index slot
	value storage.IndexSlot
	// separate chaining
	next *ListNode
}

func (n *ListNode) Append(node *ListNode) {
	for next := n.next; next.next != nil; next = next.next {
		next.next = node
	}
}

type Dirt struct {
	m map[uint32]*ListNode
}

func NewDirt() *Dirt {
	d := new(Dirt)
	d.m = make(map[uint32]*ListNode, 0)
	return d
}

func (d *Dirt) add(slot storage.IndexSlot) {

	node := new(ListNode)
	node.key = slot.GetFileId()
	node.value = slot
	node.next = nil

	if d.m[slot.GetFileId()] == nil {
		// insert directly
		d.m[slot.GetFileId()] = node
	} else {
		// append to the rear
		d.m[slot.GetFileId()].Append(node)

	}
}
