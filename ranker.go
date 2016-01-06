package search

//排序器基类

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
