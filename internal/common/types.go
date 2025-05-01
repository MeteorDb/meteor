package common

type K []byte
type V []byte

type WalRow struct {
}

type Command struct {
	Operation string   `json:"operation"`
	Args      []string `json:"args"`
}
