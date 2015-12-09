package search

import (
	"github.com/aosen/cut"
	"log"
	"testing"
)

//初始化搜索引擎
var searcher Engine

//初始化分词
var segmenter cut.Segmenter

func TestSearch(t *testing.T) {
	segmenter.LoadDictionary("data/dict.txt")
	searcher.Init(EngineInitOptions{
		Segmenter:            segmenter,
		UsePersistentStorage: true,
		IndexerInitOptions: &IndexerInitOptions{
			IndexType: LocationsIndex,
			BM25Parameters: &BM25Parameters{
				K1: 2.0,
				B:  0.75,
			},
		},
		PersistentStorageShards: 4,
	})
	defer searcher.Close()

	// 将文档加入索引
	searcher.IndexDocument(0, DocumentIndexData{Content: "此次百度收购将成中国互联网最大并购"})
	searcher.IndexDocument(1, DocumentIndexData{Content: "百度宣布拟全资收购91无线业务"})
	searcher.IndexDocument(2, DocumentIndexData{Content: "百度是中国最大的搜索引擎"})
	searcher.FlushIndex()

	// 搜索输出格式见types.SearchResponse结构体
	log.Print(searcher.Search(SearchRequest{Text: "百度中国"}))
}
