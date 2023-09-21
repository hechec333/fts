package codec

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"testing"
	"time"
)

type IoObejct struct {
	Name string
	Data []int64
}

func GenIoObeject(name string, r int) *IoObejct {

	ib := IoObejct{
		Name: name,
		Data: make([]int64, r),
	}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < r; i++ {
		ib.Data[i] = rand.Int63()
	}

	return &ib
}

func (i *IoObejct) Serial() []byte {
	buf := new(bytes.Buffer)

	e := gob.NewEncoder(buf)

	e.Encode(i)
	return buf.Bytes()
}

func (i *IoObejct) Dump(b []byte) {
	r := bytes.NewReader(b)

	d := gob.NewDecoder(r)

	d.Decode(i)

}

func TestGzipCodec(t *testing.T) {

	obj := GenIoObeject("tokenzzz", 500000)
	bs := obj.Serial()

	codec := NewGzipCodec()
	buf := new(bytes.Buffer)
	codec.BindW(buf)
	codec.Encode(bs)
	rate := float64(len(buf.Bytes())) / float64(len(bs))
	t.Logf("before compress len %v,after compress len %v,rate %v", len(bs), len(buf.Bytes()), rate)
}
