package bigsort

type BigSortable interface {
	Less(other BigSortable) bool
	Deserialize(data []byte) error
	Serialize(buf []byte) []byte
	Size() int64 // Size in bytes of the value, used to determine when swap to a new buffer
}
