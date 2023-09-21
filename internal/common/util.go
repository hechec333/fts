package common

import (
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fts/internal/plat"
	"fts/internal/types"
	"hash/crc32"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func IsExist(f string) bool {
	_, err := os.Stat(f)
	return err == nil || os.IsExist(err)
}
func GetInt64() int64 {
	return time.Now().UnixNano()
}

func GetSha256(data []byte) string {
	c := sha256.New()
	c.Write(data)
	return hex.EncodeToString(c.Sum(nil))
}
func GetCrc32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
func GetFileSize(f *os.File) int64 {
	f.Sync()
	st, err := f.Stat()
	if err != nil {
		panic(err)
	}
	return st.Size()
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func Int64WithInt16var(anchor int16) int64 {
	rand.Seed(time.Now().UnixNano())
	x := rand.Int63()

	r := int64(anchor) << 48

	return x | r
}

func Int64GetInt16var(d int64) int16 {
	y := d >> 48
	return int16(y)
}
func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func BinarySearch(dt []int64, t int64) bool {
	low, high := 0, len(dt)-1
	for low <= high {
		mid := (low + high) / 2
		if dt[mid] == t {
			return true
		} else if dt[mid] < t {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return false
}

const (
	a = 6364136223846793005
	c = 1442695040888963407
)

func lcg(seed int64) int64 {
	return a*seed + c
}

func StringHashToInt64(s string) int64 {
	var hash int64
	for _, c := range s {
		hash = hash*31 + int64(c)
	}
	return lcg(hash)
}

func GetUnionSet(a []int64, b []int64) []int64 {
	var unionSet []int64
	pA := 0
	pB := 0
	for pA < len(a) && pB < len(b) {
		if a[pA] < b[pB] {
			unionSet = append(unionSet, a[pA])
			pA++
		} else if b[pB] < a[pA] {
			unionSet = append(unionSet, b[pB])
			pB++
		} else {
			unionSet = append(unionSet, a[pA])
			pA++
			pB++
		}
	}
	if pA < len(a) {
		for i := pA; i < len(a); i++ {
			unionSet = append(unionSet, a[i])
		}
	}
	if pB < len(b) {
		for i := pB; i < len(b); i++ {
			unionSet = append(unionSet, b[i])
		}
	}
	return unionSet
}

func TokenGetType(token string) (r int) {
	items := strings.Split(token, "!")
	r, _ = strconv.Atoi(items[1])
	return
}
func SpiltI64AndString(token string) (int64, string) {
	arr := strings.Split(token, "#")
	i64, _ := strconv.ParseInt(arr[0], 10, 64)
	return i64, arr[1]
}
func MergeI64AndString(i int64, s string) string {
	return strconv.FormatInt(i, 10) + "#" + s
}
func SplitDoubleString(s string) (string, string) {
	arr := strings.Split(s, "#")
	return arr[0], arr[1]
}
func MergeDoubleString(s, b string) string {
	return s + "#" + b
}

func TokenSetType(token string, t int) string {
	token += "!" + strconv.Itoa(t)
	return token
}

func TokenGet(token string) (id string, r int) {
	items := strings.Split(token, "!")
	id = items[0]
	r, _ = strconv.Atoi(items[1])
	return
}

func CopyTokenMetaArray(src []types.TokenMeta) (dst []types.TokenMeta) {
	for _, r := range src {
		dst = append(dst, r.Copy())
	}
	return
}

// a,b pre-order
func CommonSubset(a, b []int64) []int64 {
	i, j := 0, 0
	result := []int64{}
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}

	return result
}

func GetPlatFormFsBlockSize(filename string) uint64 {
	return uint64(plat.GetFsBlockSize(filename))
}

func LongestCommonPrefix(str1, str2 []rune) int {
	n := len(str1)
	m := len(str2)
	i := 0
	j := 0
	for i < n && j < m {
		if str1[i] != str2[j] {
			break
		}
		i++
		j++
	}
	if i == 0 {
		return -1
	}
	return i
}

// 步进式求数组组合子集
func KSubSet(si []interface{}, k int) [][]interface{} {
	if k <= 0 {
		k = 1
	}
	res := [][]interface{}{}
	var dfs func(int, []interface{})
	dfs = func(idx int, path []interface{}) {
		if len(path) == k {
			tmp := make([]interface{}, len(path))
			copy(tmp, path)
			res = append(res, tmp)
			return
		}
		for i := idx; i < len(si); i++ {
			path = append(path, si[i])
			dfs(i+1, path)
			path = path[:len(path)-1]
		}
	}
	dfs(0, []interface{}{})
	return res
}

// 返回至少k位的组合
func AtLeaseKSubSet(si []interface{}, k int) [][]interface{} {
	if k <= 0 {
		k = 1
	}
	res := [][]interface{}{}
	var dfs func(int, []interface{})
	dfs = func(idx int, path []interface{}) {
		if len(path) >= k {
			tmp := make([]interface{}, len(path))
			copy(tmp, path)
			res = append(res, tmp)
			return
		}
		for i := idx; i < len(si); i++ {
			path = append(path, si[i])
			dfs(i+1, path)
			path = path[:len(path)-1]
		}
	}
	dfs(0, []interface{}{})
	return res
}

func GetPlatSpace() string {
	switch runtime.GOOS {
	case "window":
		return "\r\n"
	default:
		return "\n"
	}
}

func MergeString(s ...string) string {
	return strings.Join(s, "#")
}

func SplitString(s string) []string {
	return strings.Split(s, "#")
}

func ExtractMetaTypeName(t reflect.Type) string {
	s := t.String()

	idx := strings.LastIndex(s, ".")
	if idx != -1 {
		sub := s[idx+1:]
		if sub[0] == '*' {
			return sub[1:]
		} else {
			return sub
		}
	} else {
		return s
	}
}
