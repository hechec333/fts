package py

import (
	"sync"
	"testing"
	"time"
)

func TestTag(t *testing.T) {

	TagText("据每日人物报道，一份《检举税收违法行为受理回执》显示")
}
func TestSerialCallTag(t *testing.T) {
	tme := time.Now()
	r, _ := TagText("每日人物报道")
	t.Log(r)
	// r, _ = TagText("《检举税收违法行为受理回执》")
	// t.Log(r)
	t.Log(time.Since(tme))
	time.Sleep(time.Second)

}
func TestConcurrentTag(t *testing.T) {
	mu := sync.WaitGroup{}

	mu.Add(2)
	go func() {
		defer mu.Done()
		time.Sleep(1 * time.Second)
		TagText("每日人物报道")
		t.Log(1)
	}()

	go func() {
		defer mu.Done()

		time.Sleep(1 * time.Second)
		TagText("《检举税收违法行为受理回执》")
		t.Log(2)
	}()

	// go func() {
	// 	defer mu.Done()

	// 	time.Sleep(1 * time.Second)
	// 	TagText("《检举》")
	// 	t.Log(3)
	// }()

	mu.Wait()

}
