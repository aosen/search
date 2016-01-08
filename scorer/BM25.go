/*
Author: Aosen
Data: 2016-01-06
QQ: 316052486
Desc: BM25打分器
详情见http://en.wikipedia.org/wiki/Okapi_BM25
*/
package scorer

import "github.com/aosen/search"

// 一个简单的评分规则，文档分数为BM25
type BM25Scorer struct {
}

func NewBM25Scorer() *BM25Scorer {
	return &BM25Scorer{}
}

func (self *BM25Scorer) computeBM25(doc search.IndexedDocument, fields interface{}) {

}

func (self *BM25Scorer) Score(doc search.IndexedDocument, fields interface{}) []float32 {
	return []float32{doc.BM25}
}
