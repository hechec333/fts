package main

/*
#cgo pkg-config: python-3.10
#include <Python.h>
*/
import "C"
import (
	"fmt"
	"time"
)

func main() {
	C.Py_Initialize()
	// 导入 SnowNLP 模块
	snowNLPModule := C.PyImport_ImportModule(C.CString("snownlp"))
	defer C.Py_DecRef(snowNLPModule)
	// 获取 SnowNLP 类
	snowNLPClass := C.PyObject_GetAttrString(snowNLPModule, C.CString("SnowNLP"))
	defer C.Py_DecRef(snowNLPClass)
	// 创建一个 SnowNLP 对象
	// runtime.LockOSThread()
	// defer runtime.UnlockOSThread()
	ti := time.Now()
	text := C.CString("这是一个测试")
	args := C.PyTuple_New(1)
	defer C.Py_DecRef(args)
	C.PyTuple_SetItem(args, 0, C.PyUnicode_FromString(text))
	snowNLPObj := C.PyObject_CallObject(snowNLPClass, args)
	defer C.Py_DecRef(snowNLPObj)
	tags := C.PyObject_GetAttrString(snowNLPObj, C.CString("listTags"))
	fmt.Println("get tags")
	defer C.Py_DecRef(tags)
	// tags := C.PyObject_CallObject(tagsMethod, nil)
	// defer C.Py_DecRef(tags)
	fmt.Println("call tags")
	//遍历结果并输出
	size := C.PyList_Size(tags)
	for i := 0; i < int(size); i++ {
		item := C.PyList_GetItem(tags, C.Py_ssize_t(i))
		word := C.PyTuple_GetItem(item, 0)
		tag := C.PyTuple_GetItem(item, 1)
		fmt.Printf("%s/%s ", C.GoString(C.PyUnicode_AsUTF8(word)), C.GoString(C.PyUnicode_AsUTF8(tag)))
		// C.Py_DecRef(word)
		// C.Py_DecRef(tag)
		// C.Py_DecRef(item)
	}

	fmt.Printf("segment cost time %v", time.Since(ti))
	C.Py_Finalize()
}
