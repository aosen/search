package search

import ()

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
