package store

type Store struct {
	store   map[string]string // For now created map with string, should use a custom map with custom object value
	expires map[string]uint64
	numKeys int
	ShardID int
}
