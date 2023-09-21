package common

import (
	"bytes"
	"encoding/binary"
)

func Uint16ToBytes(i uint16) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, i)
	return b.Bytes()
}
func Uint32ToBytes(i uint32) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, i)
	return b.Bytes()
}

func Uint64ToBytes(i uint64) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, i)
	return b.Bytes()
}

func Int16ToBytes(i int16) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, i)
	return b.Bytes()
}
func Int32Tobytes(i int32) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, i)
	return b.Bytes()
}

func Int64ToBytes(i int64) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, i)
	return b.Bytes()
}

func Float64ToBytes(f float64) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, f)
	return b.Bytes()
}

func Float32ToBytes(f float32) []byte {
	b := new(bytes.Buffer)
	binary.Write(b, binary.LittleEndian, f)
	return b.Bytes()
}

func BytesToUint16(b []byte) (i uint16) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &i)
	return
}
func BytesToUint32(b []byte) (i uint32) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &i)
	return
}
func BytesToUint64(b []byte) (i uint64) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &i)
	return
}

func BytesToInt16(b []byte) (i int16) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &i)
	return
}
func BytesToInt32(b []byte) (i int32) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &i)
	return
}
func BytesToInt64(b []byte) (i int64) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &i)
	return
}
func BytesToFloat32(b []byte) (f float32) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &f)
	return
}
func BytesToFloat64(b []byte) (f float64) {
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &f)
	return
}
