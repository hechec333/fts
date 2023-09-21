package codec

import "io"

type CountReader struct {
	r     io.Reader
	count int64
}
type CountWriter struct {
	w     io.Writer
	count int64
}

func NewCountReader(r io.Reader) *CountReader {
	return &CountReader{
		r:     r,
		count: 0,
	}
}

func NewCountWriter(w io.Writer) *CountWriter {
	return &CountWriter{
		w:     w,
		count: 0,
	}
}

func (cr *CountReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.count += int64(n)
	return n, err
}
func (cw *CountWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.count += int64(n)
	return n, err
}
