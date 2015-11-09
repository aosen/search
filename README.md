# search
Golang实现的搜索引擎

详细用法例见: github.com/aosen/searchserver

#几个重要结构体
##搜索引擎初始化结构体
```Golang
type EngineInitOptions struct {
	// 半角逗号分隔的字典文件，具体用法见
	// sego.Segmenter.LoadDictionary函数的注释
	Segmenter cut.Segmenter

	// 停用词文件
	StopTokenFile string

	// 分词器线程数
	NumSegmenterThreads int

	// 索引器和排序器的shard数目
	// 被检索/排序的文档会被均匀分配到各个shard中
	NumShards int

	// 索引器的信道缓冲长度
	IndexerBufferLength int

	// 索引器每个shard分配的线程数
	NumIndexerThreadsPerShard int

	// 排序器的信道缓冲长度
	RankerBufferLength int

	// 排序器每个shard分配的线程数
	NumRankerThreadsPerShard int

	// 索引器初始化选项
	IndexerInitOptions *IndexerInitOptions

	// 默认的搜索选项
	DefaultRankOptions *RankOptions

	// 是否使用持久数据库，以及数据库文件保存的目录和裂分数目
	UsePersistentStorage    bool
	PersistentStorageFolder string
	PersistentStorageShards int
}
```
##建立索引结构体
```Golang
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
```
##建立索引
```Golang
func (engine *Engine) IndexDocument(docId uint64, data DocumentIndexData)
```
##搜索请求结构体
```Golang
type SearchRequest struct {
	// 搜索的短语（必须是UTF-8格式），会被分词
	// 当值为空字符串时关键词会从下面的Tokens读入
	Text string

	// 关键词（必须是UTF-8格式），当Text不为空时优先使用Text
	// 通常你不需要自己指定关键词，除非你运行自己的分词程序
	Tokens []string

	// 文档标签（必须是UTF-8格式），标签不存在文档文本中，但也属于搜索键的一种
	Labels []string

	// 当不为空时，仅从这些文档中搜索
	DocIds []uint64

	// 排序选项
	RankOptions *RankOptions

	// 超时，单位毫秒（千分之一秒）。此值小于等于零时不设超时。
	// 搜索超时的情况下仍有可能返回部分排序结果。
	Timeout int
}
```
##搜索查询符合条件的文档
```Golang
func (engine *Engine) Search(request SearchRequest) (output SearchResponse)
```
##搜索引擎返回结构体
```Golang
type SearchResponse struct {
	// 搜索用到的关键词
	Tokens []string

	// 搜索到的文档，已排序
	Docs []ScoredDocument

	// 搜索是否超时。超时的情况下也可能会返回部分结果
	Timeout bool
}
```
