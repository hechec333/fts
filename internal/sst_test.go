package internal

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSST(t *testing.T) {

	data := []Record{
		{
			Suffix: "ssxs",
			ID:     0x1023,
		}, {
			Suffix: "sfafs",
			ID:     0x1023,
		}, {
			Suffix: "fkalf",
			ID:     0x1023,
		}, {
			Suffix: "玲珑",
			ID:     0x1023,
		}, {
			Suffix: "发发",
			ID:     0x1023,
		},
	}
	sst := NewSST(0)
	for _, v := range data {
		sst.InsertRecord(v)
	}

	n := rand.Intn(len(data))
	assert.Equal(t, data[n], *sst.SearchRecord(data[n].Suffix))
}
