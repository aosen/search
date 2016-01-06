/*
Author: Aosen
Data: 2016-01-06
QQ: 316052486
Desc: 采用悟空排序器，源码参考：https://github.com/aosen/wukong
*/

package ranker

import (
	"log"
	"sort"
	"sync"

	"github.com/aosen/search"
	"github.com/aosen/search/utils"
)

type WuKongRanker struct {
	lock struct {
		sync.RWMutex
		fields map[uint64]interface{}
	}
	initialized bool
}

func NewWuKongRanker() *WuKongRanker {
	return &WuKongRanker{}
}

//排序起初始化
func (self *WuKongRanker) Init() {
	if self.initialized == true {
		log.Fatal("排序器不能初始化两次")
	}
	self.initialized = true

	self.lock.fields = make(map[uint64]interface{})
}

// 给某个文档添加评分字段
func (self *WuKongRanker) AddScoringFields(docId uint64, fields interface{}) {
	if self.initialized == false {
		log.Fatal("排序器尚未初始化")
	}

	self.lock.Lock()
	self.lock.fields[docId] = fields
	self.lock.Unlock()
}

// 删除某个文档的评分字段
func (self *WuKongRanker) RemoveScoringFields(docId uint64) {
	if self.initialized == false {
		log.Fatal("排序器尚未初始化")
	}

	self.lock.Lock()
	delete(self.lock.fields, docId)
	self.lock.Unlock()
}

// 给文档评分并排序
func (self *WuKongRanker) Rank(
	docs []search.IndexedDocument, options search.RankOptions) (outputDocs search.ScoredDocuments) {
	if self.initialized == false {
		log.Fatal("排序器尚未初始化")
	}

	// 对每个文档评分
	for _, d := range docs {
		self.lock.RLock()
		fs := self.lock.fields[d.DocId]
		self.lock.RUnlock()
		// 计算评分并剔除没有分值的文档
		scores := options.SearchScorer.Score(d, fs)
		if len(scores) > 0 {
			outputDocs = append(outputDocs, search.ScoredDocument{
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
