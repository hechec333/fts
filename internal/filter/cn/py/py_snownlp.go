package py

/*
#cgo pkg-config: python-3.10
#include <Python.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"fts/internal/common"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

type PyTaskStat struct {
	Seq   int64
	Func  string
	Args  []interface{}
	Reply interface{}
	Err   error
}

var (
	state          *C.PyThreadState = nil
	module         *C.PyObject      = nil
	mu                              = sync.Mutex{}
	seed           int64            = 0x33ff
	taskCh         chan *PyTaskStat = nil
	waitCh         map[int64]chan *PyTaskStat
	stopCh         chan struct{}
	methods        map[string]*C.PyObject
	ErrTaskTimeOut = errors.New("I/O timeout")
)

func init() {
	sigs := make(chan os.Signal, 1)
	taskCh = make(chan *PyTaskStat)
	waitCh = make(map[int64]chan *PyTaskStat)
	stopCh = make(chan struct{})
	methods = make(map[string]*C.PyObject)
	// 注册信号处理函数
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSEGV)
	go func() {
		common.INFO("Init Python Env")
		initPyEnv()
		for {
			select {
			case sig := <-sigs:
				fmt.Println(sig)
			case <-stopCh:
				releasePyEnv(true)
				return
			case st := <-taskCh:
				run(st)
				if _, ok := waitCh[st.Seq]; ok {
					waitCh[st.Seq] <- st
				}
			}
		}
	}()

}

func initPyEnv() {
	runtime.LockOSThread()
	if C.Py_IsInitialized() != 1 {
		C.Py_Initialize()
		module = C.PyImport_ImportModule(C.CString("x"))
		methods["TagText"] = C.PyObject_GetAttrString(module, C.CString("TagText"))
		state = C.PyEval_SaveThread()
	}
}

func releasePyEnv(is bool) {
	runtime.UnlockOSThread()
	if C.Py_IsInitialized() == 1 {
		C.PyEval_RestoreThread(state)
		C.Py_DecRef(module)
		C.Py_Finalize()
	}
}

func run(st *PyTaskStat) {
	switch st.Func {
	case "TagText":
		tagText(st.Args[0].(string))
		res, err := tagText(st.Args[0].(string))
		st.Err = err
		st.Reply = res
	default:
		return
	}
}

type TaggerResult struct {
	Tokens string
	NNS    string
}

func Close() {
	stopCh <- struct{}{}
}

// 本地路径问题
func TagText(text string) ([]TaggerResult, error) {
	mu.Lock()
	id := seed
	seed++
	mu.Unlock()

	st := PyTaskStat{
		Seq:  id,
		Args: []interface{}{text},
		Func: "TagText",
		Err:  nil,
	}

	taskCh <- &st
	waitCh[id] = make(chan *PyTaskStat, 1)
	select {
	case <-time.After(100 * time.Second):
		return nil, ErrTaskTimeOut
	case <-waitCh[id]:
		return st.Reply.([]TaggerResult), st.Err
	}

}

func tagText(text string) ([]TaggerResult, error) {
	var (
		tagger []TaggerResult
		err    error
	)
	_gstate := C.PyGILState_Ensure()
	defer C.PyGILState_Release(_gstate)

	method := methods["TagText"]
	//defer C.Py_DecRef(method)
	if C.PyCallable_Check(method) == 0 {
		panic("uncatch method nil")
	}
	args := C.PyTuple_New(1)
	defer C.Py_DecRef(args)
	texts := C.PyUnicode_FromString(C.CString(text))
	defer C.Py_DecRef(texts)
	C.PyTuple_SetItem(args, 0, texts)
	obj := C.PyObject_CallObject(method, args)
	defer C.Py_DecRef(obj)
	ok := C.PyObject_GetAttrString(obj, C.CString("ok"))
	defer C.Py_DecRef(ok)
	if C.PyObject_IsTrue(obj) != 1 {
		str := C.PyObject_GetAttrString(obj, C.CString("error"))
		defer C.Py_DecRef(str)
		cstr := C.PyUnicode_AsUTF8(str)
		err = errors.New(string(C.GoString(cstr)))
	} else {
		list := C.PyObject_GetAttrString(obj, C.CString("list"))
		defer C.Py_DecRef(list)
		size := C.PyList_Size(list)
		for i := 0; i < int(size); i++ {
			item := C.PyList_GetItem(list, C.Py_ssize_t(i))
			word := C.PyTuple_GetItem(item, 0)
			tag := C.PyTuple_GetItem(item, 1)
			tagger = append(tagger, TaggerResult{
				Tokens: C.GoString(C.PyUnicode_AsUTF8(word)),
				NNS:    C.GoString(C.PyUnicode_AsUTF8(tag)),
			})
		}
	}
	//python.PyEval_SaveThread()
	return tagger, err
}
