package codec

import (
	"compress/gzip"
	"io"
)

type GzipDiskCodec struct {
	r io.Reader
	w io.Writer
}

func NewGzipCodec() *GzipDiskCodec {
	return &GzipDiskCodec{}
}

func (gdc *GzipDiskCodec) BindR(r io.Reader) {
	gdc.r = r

}
func (gdc *GzipDiskCodec) BindW(w io.Writer) {
	gdc.w = w
}
func (gdc *GzipDiskCodec) Encode(b []byte) (int64, error) {
	//buf := new(bytes.Buffer)
	cw := NewCountWriter(gdc.w)
	w := gzip.NewWriter(cw)
	defer w.Close()
	_, err := w.Write(b)
	return cw.count, err
}

func (gdc *GzipDiskCodec) Decode() ([]byte, error) {
	r, err := gzip.NewReader(gdc.r)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}
