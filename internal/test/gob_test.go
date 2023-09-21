package test_test

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestGob(t *testing.T) {

	s := struct {
		M map[string]reflect.Type
	}{}
	s.M = make(map[string]reflect.Type)
	s.M["sss"] = reflect.TypeOf(12)
	s.M["xxx"] = reflect.TypeOf("xxx")

	//gob.Register()
	//reflect.FuncOf()
	js, _ := json.Marshal(s)

	t.Log(string(js))
}
