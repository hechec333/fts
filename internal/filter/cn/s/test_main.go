package main

// #cgo pkg-config: python-3.10
// #include <Python.h>
import "C"
import "fmt"

func main() {
	C.Py_Initialize()
	fmt.Println(C.GoString(C.Py_GetVersion()))
	C.Py_Finalize()
}
