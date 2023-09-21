package cn

import (
	"bufio"
	"fmt"
	"fts/internal/types"
	"io"
	"os"
	"strings"
)

var pause = []string{}

type PauseFilter struct {
}

func (pf *PauseFilter) Gen(token []types.TokenMeta) (res []types.TokenMeta) {

	for _, v := range token {
		for _, p := range pause {
			if idx := strings.Index(v.Token(), p); idx != -1 {
				//匹配暂停词一次
				t := v.Copy()
				t.SetToken(v.Token()[:idx])
				res = append(res, t)
				t = v.Copy()
				t.SetToken(v.Token()[idx+len(p):])
				res = append(res, t)
				goto n
			}
		}
		res = append(res, v)
	n:
	}

	return
}

func loadPauseWord() {
	root := "H:/CODEfield/GO/src/project/util/fts/internal/filter/dic/cn_pause.txt"

	f, err := os.Open(root)

	if err != nil {
		panic(err)
	}
	defer f.Close()

	b := bufio.NewReader(f)
	b.ReadLine()
	for {
		line, _, err := b.ReadLine()

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return
			}
		}
		pause = append(pause, string(line))
	}
	fmt.Printf("loading %v pause words", len(pause))
}
