package search

//排序器基类

import (
	"github.com/aosen/search/utils"
	"log"
	"sort"
	"sync"
)

//排序起接口
type SearchRanker interface {
	//排序起初始化
	Init()
	// 给某个文档添加评分字段
	AddScoringFields(docId uint64, fields interface{})
	// 删除某个文档的评分字段
	RemoveScoringFields(docId uint64)
	// 给文档评分并排序
	Rank(docs []IndexedDocument, options RankOptions) (outputDocs ScoredDocuments)
}

type RankOptions struct {
	// 文档的评分规则，值为nil时使用Engine初始化时设定的规则
	ScoringCriteria ScoringCriteria

	// 默认情况下（ReverseOrder=false）按照分数从大到小排序，否则从小到大排序
	ReverseOrder bool

	// 从第几条结果开始输出
	OutputOffset int

	// 最大输出的搜索结果数，为0时无限制
	MaxOutputs int
}

type Ranker struct {
	lock struct {
		sync.RWMutex
		fields map[uint64]interface{}
	}
	initialized bool
}

//排序起初始化
func (ranker *Ranker) Init() {
	if ranker.initialized == true {
		log.Fatal("排序器不能初始化两次")
	}
	ranker.initialized = true

	ranker.lock.fields = make(map[uint64]interface{})
}

// 给某个文档添加评分字段
func (ranker *Ranker) AddScoringFields(docId uint64, fields interface{}) {
	if ranker.initialized == false {
		log.Fatal("排序器尚未初始化")
	}

	ranker.lock.Lock()
	ranker.lock.fields[docId] = fields
	ranker.lock.Unlock()
}

// 删除某个文档的评分字段
func (ranker *Ranker) RemoveScoringFields(docId uint64) {
	if ranker.initialized == false {
		log.Fatal("排序器尚未初始化")
	}

	ranker.lock.Lock()
	delete(ranker.lock.fields, docId)
	ranker.lock.Unlock()
}

// 给文档评分并排序
func (ranker *Ranker) Rank(
	docs []IndexedDocument, options RankOptions) (outputDocs ScoredDocuments) {
	if ranker.initialized == false {
		log.Fatal("排序器尚未初始化")
	}

	// 对每个文档评分
	for _, d := range docs {
		ranker.lock.RLock()
		fs := ranker.lock.fields[d.DocId]
		ranker.lock.RUnlock()
		// 计算评分并剔除没有分值的文档
		scores := options.ScoringCriteria.Score(d, fs)
		if len(scores) > 0 {
			outputDocs = append(outputDocs, ScoredDocument{
				DocId:                 d.DocId,
				Scores:                scores,
				TokenSnippetLocations: d.TokenSnippetLocations,
				TokenLocations:        d.TokenLocations})
		}
	}

	// 排序
	if options.ReverseOrder {
		sort.Sort(sort.Reverse(outputDocs))
	} else {
		sort.Sort(outputDocs)
	}

	// 当用户要求只返回部分结果时返回部分结果
	var start, end int
	if options.MaxOutputs != 0 {
		start = utils.MinInt(options.OutputOffset, len(outputDocs))
		end = utils.MinInt(options.OutputOffset+options.MaxOutputs, len(outputDocs))
	} else {
		start = utils.MinInt(options.OutputOffset, len(outputDocs))
		end = len(outputDocs)
	}
	return outputDocs[start:end]
}

// 见http://en.wikipedia.org/wiki/Okapi_BM25
// 默认值见engine_init_options.go
type BM25Parameters struct {
	K1 float32
	B  float32
}

// 一个简单的评分规则，文档分数为BM25
type RankByBM25 struct {
}

func (rule RankByBM25) Score(doc IndexedDocument, fields interface{}) []float32 {
	return []float32{doc.BM25}
}
