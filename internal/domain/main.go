package domain

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
