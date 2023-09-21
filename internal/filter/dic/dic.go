package dic

import (
	"bufio"
	"fts/internal"
	"fts/internal/common"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	p, _ := os.Getwd()
	common.INFO(p)
	_, f, _, _ := runtime.Caller(1)
	common.INFO(f)
	common.INFO("Loading Stopwords Dictionary...")
	t := time.Now()
	paths := strings.Split(dic_path, ";")
	loadparts(paths)
	common.INFO("Complete Loading Dictionary in %v", time.Since(t))
	types := []string{}
	keys := []string{}
	for k, v := range stopWordDic {
		types = append(types, k)
		keys = append(keys, strconv.Itoa(int(v.bloom.KeySize())))
	}
	common.INFO("Loading types: %v,size: %v", strings.Join(types, "/"), strings.Join(keys, "/"))
}

var base = "H:/CODEfield/GO/src/project/util/fts/internal/filter/dic/"
var dic_path = `cn_stopwords_*.txt;en_stopwords.txt;cnname_.txt`
var stopWordDic map[string]*StopWordsDic

type StopWordsDic struct {
	bloom *internal.Filter
}

func (sd *StopWordsDic) AddWords(s string) {
	sd.bloom.AddString(s)
}
func (sd *StopWordsDic) TestWords(s string) bool {
	return sd.bloom.TestString(s)
}

func getPrefix(s string) string {
	idx := strings.Index(s, "_")
	return s[:idx]
}

func LoadDic(pre string) *StopWordsDic {
	var (
		sd *StopWordsDic
		ok bool
	)
	if sd, ok = stopWordDic[pre]; !ok {
		load(pre)
	}
	sd = stopWordDic[pre]

	return sd
}

func load(pre string) {
	paths := strings.Split(dic_path, ";")
	s := []string{}
	for _, v := range paths {
		if pre == getPrefix(v) {
			s = append(s, v)
		}
	}
	if len(s) != 0 {
		loadparts(s)
	}
}

func loadparts(paths []string) {
	files := make(map[string][]string)
	lines := make(map[string]int64)
	for _, v := range paths {
		if strings.Contains(v, "*") {
			//adjust regex
			v = strings.ReplaceAll(v, ".", `\.`)
			v = strings.ReplaceAll(v, "*", ".*")
			reg, err := regexp.Compile(v)
			if err != nil {
				panic(err)
			}
			filepath.Walk(base, func(path string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && reg.Match([]byte(info.Name())) {
					f, _ := os.Open(base + info.Name())
					defer f.Close()

					r := bufio.NewReader(f)
					bs, _, _ := r.ReadLine()
					if string(bs[:2]) != "//" {
						return nil
					}
					pre := getPrefix(info.Name())
					if _, ok := files[pre]; !ok {
						files[pre] = []string{info.Name()}
					} else {
						files[pre] = append(files[pre], info.Name())
					}
					line, _ := strconv.Atoi(string(bs[3:]))
					lines[pre] += int64(line)
				}
				return nil
			})
		} else {
			f, err := os.Open(base + v)
			if err == nil {
				defer f.Close()
				r := bufio.NewReader(f)
				bs, _, _ := r.ReadLine()
				if string(bs[:2]) != "//" {
					return
				}
				words, _ := strconv.Atoi(string(bs[3:]))
				pre := getPrefix(v)
				if _, ok := files[pre]; !ok {
					files[pre] = []string{v}
				} else {
					files[pre] = append(files[pre], v)
				}
				lines[pre] += int64(words)
			}
		}
	}
	for k, size := range lines {
		size = size * 8
		if size < 1*1024*1024 {
			size = 1024 * 1024
		}
		if stopWordDic == nil {
			stopWordDic = make(map[string]*StopWordsDic)
		}
		stopWordDic[k] = &StopWordsDic{
			bloom: internal.New(uint64(size), 4, true),
		}
	}
	wg := sync.WaitGroup{}
	for k, s := range files {
		wg.Add(len(s))
		for _, v := range s {
			go func(p string, kk string) {
				defer wg.Done()
				f, _ := os.Open(base + p)
				defer f.Close()

				r := bufio.NewReader(f)
				// skip line code
				r.ReadLine()
				sd := stopWordDic[kk]
				for {
					line, _, err := r.ReadLine()
					if err != nil {
						if err == io.EOF {
							break
						}
						panic(err)
					}
					sd.bloom.AddString(string(line))
				}
			}(v, k)
		}
	}

	wg.Wait()
}
