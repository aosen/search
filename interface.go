package search

import ()

//索引器接口
type SearchIndexer interface {
	// 初始化索引器
	Init(options IndexerInitOptions)
	// 向反向索引表中加入一个文档
	AddDocument(document *DocumentIndex)
	// 查找包含全部搜索键(AND操作)的文档
	// 当docIds不为nil时仅从docIds指定的文档中查找
	Lookup() (docs []IndexedDocument)
}

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

// 评分规则通用接口
type ScoringCriteria interface {
	// 给一个文档评分，文档排序时先用第一个分值比较，如果
	// 分值相同则转移到第二个分值，以此类推。
	// 返回空切片表明该文档应该从最终排序结果中剔除。
	Score(doc IndexedDocument, fields interface{}) []float32
}
