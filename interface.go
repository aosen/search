package search

import ()

//引擎接口
type SearchEngine interface {
	//引擎初始化
	Init(options EngineInitOptions)
	//索引文档
	IndexDocument(docId uint64, data DocumentIndexData)
	//将文档从索引中删除
	RemoveDocument(docId uint64)
	// 阻塞等待直到所有索引添加完毕
	FlushIndex()
	// 查找满足搜索条件的文档，此函数线程安全
	Search(request SearchRequest) (output SearchResponse)
	//Tokens的数量
	NumTokenIndexAdded() uint64
	//文档的数量
	NumDocumentsIndexed() uint64
	//关闭引擎
	Close()
}

//分词器
type SearchSegmenter interface {
	// 返回分词器使用的词典
	Dictionary() *Dictionary
	// 从文件中载入词典
	// 可以载入多个词典文件，文件名用","分隔，排在前面的词典优先载入分词，比如
	// 	"用户词典.txt,通用词典.txt"
	// 当一个分词既出现在用户词典也出现在通用词典中，则优先使用用户词典。
	// 词典的格式为（每个分词一行）：
	//	分词文本 频率 词性
	LoadDictionary(files string)
	// 对文本分词
	// 输入参数：
	//	bytes	UTF8文本的字节数组
	// 输出：
	//	[]Segment	划分的分词
	Cut(bytes []byte, model bool) []Segment
}

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

//存储器
type SearchPipline interface {
	//初始化存储器, shard为初始化的集合编号
	Init(shard int)
	//存储索引
	Set(shard int)
	//从DB删除索引
	Delete(shard int)
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
