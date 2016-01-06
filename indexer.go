package search

//搜索引擎的索引器接口,以及提供的必要结构体和方法

//索引器接口
//开发者只要实现以下接口，即可实现一个索引器
type SearchIndexer interface {
	// 初始化索引器
	Init(options IndexerInitOptions)
	// 向反向索引表中加入一个文档
	AddDocument(document *DocumentIndex)
	// 查找包含全部搜索键(AND操作)的文档
	// 当docIds不为nil时仅从docIds指定的文档中查找
	Lookup(tokens []string, labels []string, docIds []uint64) (docs []IndexedDocument)
}

// 这些常数定义了反向索引表存储的数据类型
const (
	// 仅存储文档的docId
	DocIdsIndex = 0

	// 存储关键词的词频，用于计算BM25
	FrequenciesIndex = 1

	// 存储关键词在文档中出现的具体字节位置（可能有多个）
	// 如果你希望得到关键词紧邻度数据，必须使用LocationsIndex类型的索引
	LocationsIndex = 2
)

// 反向索引项，这实际上标注了一个（搜索键，文档）对。
type KeywordIndex struct {
	// 搜索键的UTF-8文本
	Text string

	// 搜索键词频
	Frequency float32

	// 搜索键在文档中的起始字节位置，按照升序排列
	Starts []int
}

type DocumentIndex struct {
	// 文本的DocId
	DocId uint64

	// 文本的关键词长
	TokenLength float32

	// 加入的索引键
	Keywords []KeywordIndex
}

type DocumentIndexData struct {
	// 文档全文（必须是UTF-8格式），用于生成待索引的关键词
	Content string

	// 文档的关键词
	// 当Content不为空的时候，优先从Content中分词得到关键词。
	// Tokens存在的意义在于绕过内置的分词器，在引擎外部
	// 进行分词和预处理。
	Tokens []TokenData

	// 文档标签（必须是UTF-8格式），比如文档的类别属性等，这些标签并不出现在文档文本中
	Labels []string

	// 文档的评分字段，可以接纳任何类型的结构体
	Fields interface{}
}

// 索引器返回结果
type IndexedDocument struct {
	DocId uint64

	// BM25，仅当索引类型为FrequenciesIndex或者LocationsIndex时返回有效值
	BM25 float32

	// 关键词在文档中的紧邻距离，紧邻距离的含义见computeTokenProximity的注释。
	// 仅当索引类型为LocationsIndex时返回有效值。
	TokenProximity int32

	// 紧邻距离计算得到的关键词位置，和Lookup函数输入tokens的长度一样且一一对应。
	// 仅当索引类型为LocationsIndex时返回有效值。
	TokenSnippetLocations []int

	// 关键词在文本中的具体位置。
	// 仅当索引类型为LocationsIndex时返回有效值。
	TokenLocations [][]int
}

// 初始化索引器选项
type IndexerInitOptions struct {
	// 索引表的类型，见上面的常数
	IndexType int

	// BM25参数
	BM25Parameters *BM25Parameters
}
