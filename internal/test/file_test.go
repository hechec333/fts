package test_test

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestRandomWriteFile(t *testing.T) {
	f, _ := os.OpenFile("test_random.txt", os.O_CREATE|os.O_RDWR, 0777)
	l := 1024 * 1024 * 16
	b := make([]byte, l)
	temp := []byte{'a', 'b', 'c', 'd', 'e'}
	for i := 0; i < l; i++ {
		b[i] = temp[i%len(temp)]
	}

	now := time.Now()

	t.Log("random I/O")
	sl := l / 4
	wg := sync.WaitGroup{}
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(seq int) {
			f.WriteAt(b[seq*sl:seq*sl+sl], int64(seq*sl))
			//f.Sync()
			wg.Done()
		}(i)
	}
	wg.Wait()
	f.Sync()

	t.Logf("cost ms:%v", time.Since(now).Milliseconds())

}

func TestSequcenWriteFile(t *testing.T) {
	f, _ := os.OpenFile("test_sequence.txt", os.O_CREATE|os.O_RDWR, 0777)
	l := 1024 * 1024 * 16
	b := make([]byte, l)
	temp := []byte{'a', 'b', 'c', 'd', 'e'}
	for i := 0; i < l; i++ {
		b[i] = temp[i%len(temp)]
	}

	now := time.Now()
	t.Log("sequence I/O")
	f.Write(b)
	f.Sync()

	t.Logf("cost ms:%v", time.Since(now).Milliseconds())
}
