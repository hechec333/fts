package main

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"fts/internal/disk"
	"fts/internal/document"
	"fts/internal/engine"
	"fts/internal/filter/cn"
	"fts/internal/index"
	"fts/internal/indexer"
	"fts/internal/query"
	"fts/internal/tokenizer"
	"io"
	"os"
	"runtime"
	"testing"
)

func TestSinaToXml(t *testing.T) {

	raw := "H:/dataset/THUCNews"
	out := "H:/dataset/xmlTHUCNews"

	loader := NewTxtSinaDocLoader(raw)

	err := loader.TransferDocToXml(out)
	if err != nil {
		t.Fatal(err)
	}

}

func TestSinaNewsFile(t *testing.T) {
	raw := "H:/dataset/THUCNews/财经/798977.txt"

	f, _ := os.Open(raw)
	defer f.Close()
	b, _ := io.ReadAll(f)

	r := bufio.NewReader(bytes.NewReader(b))
	line, _, _ := r.ReadLine()

	t.Log(string(line))
	t.Log(string(b[len(line):]))
}

func TestSinaXmlRead(t *testing.T) {
	raw := "H:/dataset/xmlTHUCNews/星座.xml"

	f, _ := os.Open(raw)

	defer f.Close()

	d := xml.NewDecoder(f)

	var doc SinaDocument

	d.Decode(&doc)

	t.Log(doc)

}

func TestSinaDocumentBuild(t *testing.T) {
	// modified your own source file path
	meta := "H:/CODEfield/GO/src/project/util/fts/example/sina"
	target := "H:/dataset/THUCNews"
	disk := disk.NewDocDiskManager(meta)
	docm := document.NewDocumentManager(64, disk)
	loader := NewTxtSinaDocLoader(target)

	docm.LoadDocument(loader)

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	numGC := stats.NumGC
	// 获取gc的总时间
	totalGCTime := stats.PauseTotalNs
	// 获取gc的平均时间
	avgGCTime := totalGCTime / uint64(numGC)
	// 打印结果
	t.Logf("GC times: %d\n", numGC)
	t.Logf("Total GC time: %d ns\n", totalGCTime)
	t.Logf("Average GC time: %d ns\n", avgGCTime)
}

func TestSinaTitleIndexBuild(t *testing.T) {
	var (
		root  = "H:/CODEfield/GO/src/project/util/fts/example/sina"
		field = "Title"
	)
	disk := disk.NewDocDiskManager(root)
	docm := document.NewDocumentManager(64, disk)
	idr := NewSinaIndexBuilder(field)
	idr.UseFilter(&cn.JiebaNounsFilter{})
	im := indexer.NewIndexerManager(root, idr)

	inm := index.NewBPIndexManager(root)

	im.BuildIndex(&SinaDocument{}, field, docm, inm)
}

func TestSinaQuery(t *testing.T) {
	var (
		root  = "H:/CODEfield/GO/src/project/util/fts/example/sina"
		field = "Title"
	)

	var (
		disk = disk.NewDocDiskManager(root)
		idr  = NewSinaIndexBuilder(field)
		inm  = index.NewBPIndexManager(root)

		tokenizer = &tokenizer.ZhTokenizer{}

		q = query.NewQueryBuilder(tokenizer, field)

		bm25 = query.NewBM25Ranker(1, 0.75, 0.5)
	)

	tokenizer.UseFilter(&cn.JiebaNounsFilter{})

	eig := engine.NewFTSEngine(
		root,
		disk,
		inm,
		q,
		bm25,
		idr,
	)

	rts, err := eig.QueryAnd("娱乐圈年度大瓜", field)
	if err != nil {
		panic(err)
	}

	for _, v := range rts {
		fmt.Println(v)
	}
}
