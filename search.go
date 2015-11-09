package search

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/aosen/cut"
	"github.com/cznic/kv"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

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

type DocumentIndex struct {
	// 文本的DocId
	DocId uint64

	// 文本的关键词长
	TokenLength float32

	// 加入的索引键
	Keywords []KeywordIndex
}

// 反向索引项，这实际上标注了一个（搜索键，文档）对。
type KeywordIndex struct {
	// 搜索键的UTF-8文本
	Text string

	// 搜索键词频
	Frequency float32

	// 搜索键在文档中的起始字节位置，按照升序排列
	Starts []int
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

// 文档的一个关键词
type TokenData struct {
	// 关键词的字符串
	Text string

	// 关键词的首字节在文档中出现的位置
	Locations []int
}

var (
	// EngineInitOptions的默认值
	defaultNumSegmenterThreads       = runtime.NumCPU()
	defaultNumShards                 = 2
	defaultIndexerBufferLength       = runtime.NumCPU()
	defaultNumIndexerThreadsPerShard = runtime.NumCPU()
	defaultRankerBufferLength        = runtime.NumCPU()
	defaultNumRankerThreadsPerShard  = runtime.NumCPU()
	defaultDefaultRankOptions        = RankOptions{
		ScoringCriteria: RankByBM25{},
	}
	defaultIndexerInitOptions = IndexerInitOptions{
		IndexType:      FrequenciesIndex,
		BM25Parameters: &defaultBM25Parameters,
	}
	defaultBM25Parameters = BM25Parameters{
		K1: 2.0,
		B:  0.75,
	}
	defaultPersistentStorageShards = 8
)

// 初始化索引器选项
type IndexerInitOptions struct {
	// 索引表的类型，见上面的常数
	IndexType int

	// BM25参数
	BM25Parameters *BM25Parameters
}

// 见http://en.wikipedia.org/wiki/Okapi_BM25
// 默认值见engine_init_options.go
type BM25Parameters struct {
	K1 float32
	B  float32
}

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

// 评分规则通用接口
type ScoringCriteria interface {
	// 给一个文档评分，文档排序时先用第一个分值比较，如果
	// 分值相同则转移到第二个分值，以此类推。
	// 返回空切片表明该文档应该从最终排序结果中剔除。
	Score(doc IndexedDocument, fields interface{}) []float32
}

// 一个简单的评分规则，文档分数为BM25
type RankByBM25 struct {
}

func (rule RankByBM25) Score(doc IndexedDocument, fields interface{}) []float32 {
	return []float32{doc.BM25}
}

type SearchResponse struct {
	// 搜索用到的关键词
	Tokens []string

	// 搜索到的文档，已排序
	Docs []ScoredDocument

	// 搜索是否超时。超时的情况下也可能会返回部分结果
	Timeout bool
}

type ScoredDocument struct {
	DocId uint64

	// 文档的打分值
	// 搜索结果按照Scores的值排序，先按照第一个数排，如果相同则按照第二个数排序，依次类推。
	Scores []float32

	// 用于生成摘要的关键词在文本中的字节位置，该切片长度和SearchResponse.Tokens的长度一样
	// 只有当IndexType == LocationsIndex时不为空
	TokenSnippetLocations []int

	// 关键词出现的位置
	// 只有当IndexType == LocationsIndex时不为空
	TokenLocations [][]int
}

type ScoredDocuments []ScoredDocument

func (docs ScoredDocuments) Len() int {
	return len(docs)
}
func (docs ScoredDocuments) Swap(i, j int) {
	docs[i], docs[j] = docs[j], docs[i]
}
func (docs ScoredDocuments) Less(i, j int) bool {
	// 为了从大到小排序，这实际上实现的是More的功能
	for iScore := 0; iScore < MinInt(len(docs[i].Scores), len(docs[j].Scores)); iScore++ {
		if docs[i].Scores[iScore] > docs[j].Scores[iScore] {
			return true
		} else if docs[i].Scores[iScore] < docs[j].Scores[iScore] {
			return false
		}
	}
	return len(docs[i].Scores) > len(docs[j].Scores)
}

// 初始化EngineInitOptions，当用户未设定某个选项的值时用默认值取代
func (options *EngineInitOptions) Init() {
	if options.NumSegmenterThreads == 0 {
		options.NumSegmenterThreads = defaultNumSegmenterThreads
	}

	if options.NumShards == 0 {
		options.NumShards = defaultNumShards
	}

	if options.IndexerBufferLength == 0 {
		options.IndexerBufferLength = defaultIndexerBufferLength
	}

	if options.NumIndexerThreadsPerShard == 0 {
		options.NumIndexerThreadsPerShard = defaultNumIndexerThreadsPerShard
	}

	if options.RankerBufferLength == 0 {
		options.RankerBufferLength = defaultRankerBufferLength
	}

	if options.NumRankerThreadsPerShard == 0 {
		options.NumRankerThreadsPerShard = defaultNumRankerThreadsPerShard
	}

	if options.IndexerInitOptions == nil {
		options.IndexerInitOptions = &defaultIndexerInitOptions
	}

	if options.IndexerInitOptions.BM25Parameters == nil {
		options.IndexerInitOptions.BM25Parameters = &defaultBM25Parameters
	}

	if options.DefaultRankOptions == nil {
		options.DefaultRankOptions = &defaultDefaultRankOptions
	}

	if options.DefaultRankOptions.ScoringCriteria == nil {
		options.DefaultRankOptions.ScoringCriteria = defaultDefaultRankOptions.ScoringCriteria
	}

	if options.PersistentStorageShards == 0 {
		options.PersistentStorageShards = defaultPersistentStorageShards
	}
}

const (
	NumNanosecondsInAMillisecond = 1000000
	PersistentStorageFilePrefix  = "db"
)

type StopTokens struct {
	stopTokens map[string]bool
}

// 从stopTokenFile中读入停用词，一个词一行
// 文档索引建立时会跳过这些停用词
func (st *StopTokens) Init(stopTokenFile string) {
	st.stopTokens = make(map[string]bool)
	if stopTokenFile == "" {
		return
	}

	file, err := os.Open(stopTokenFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		if text != "" {
			st.stopTokens[text] = true
		}
	}

}

func (st *StopTokens) IsStopToken(token string) bool {
	_, found := st.stopTokens[token]
	return found
}

type segmenterRequest struct {
	docId uint64
	hash  uint32
	data  DocumentIndexData
}

// 索引器
type Indexer struct {
	// 从搜索键到文档列表的反向索引
	// 加了读写锁以保证读写安全
	tableLock struct {
		sync.RWMutex
		table map[string]*KeywordIndices
	}

	initOptions IndexerInitOptions
	initialized bool

	// 这实际上是总文档数的一个近似
	numDocuments uint64

	// 所有被索引文本的总关键词数
	totalTokenLength float32

	// 每个文档的关键词长度
	docTokenLengths map[uint64]float32
}

// 反向索引表的一行，收集了一个搜索键出现的所有文档，按照DocId从小到大排序。
type KeywordIndices struct {
	// 下面的切片是否为空，取决于初始化时IndexType的值
	docIds      []uint64  // 全部类型都有
	frequencies []float32 // IndexType == FrequenciesIndex
	locations   [][]int   // IndexType == LocationsIndex
}

// 初始化索引器
func (indexer *Indexer) Init(options IndexerInitOptions) {
	if indexer.initialized == true {
		log.Fatal("索引器不能初始化两次")
	}
	indexer.initialized = true

	indexer.tableLock.table = make(map[string]*KeywordIndices)
	indexer.initOptions = options
	indexer.docTokenLengths = make(map[uint64]float32)
}

// 向反向索引表中加入一个文档
func (indexer *Indexer) AddDocument(document *DocumentIndex) {
	if indexer.initialized == false {
		log.Fatal("索引器尚未初始化")
	}

	indexer.tableLock.Lock()
	defer indexer.tableLock.Unlock()

	// 更新文档关键词总长度
	if document.TokenLength != 0 {
		originalLength, found := indexer.docTokenLengths[document.DocId]
		indexer.docTokenLengths[document.DocId] = float32(document.TokenLength)
		if found {
			indexer.totalTokenLength += document.TokenLength - originalLength
		} else {
			indexer.totalTokenLength += document.TokenLength
		}
	}

	docIdIsNew := true
	for _, keyword := range document.Keywords {
		indices, foundKeyword := indexer.tableLock.table[keyword.Text]
		if !foundKeyword {
			// 如果没找到该搜索键则加入
			ti := KeywordIndices{}
			switch indexer.initOptions.IndexType {
			case LocationsIndex:
				ti.locations = [][]int{keyword.Starts}
			case FrequenciesIndex:
				ti.frequencies = []float32{keyword.Frequency}
			}
			ti.docIds = []uint64{document.DocId}
			indexer.tableLock.table[keyword.Text] = &ti
			continue
		}

		// 查找应该插入的位置
		position, found := indexer.searchIndex(
			indices, 0, indexer.getIndexLength(indices)-1, document.DocId)
		if found {
			docIdIsNew = false

			// 覆盖已有的索引项
			switch indexer.initOptions.IndexType {
			case LocationsIndex:
				indices.locations[position] = keyword.Starts
			case FrequenciesIndex:
				indices.frequencies[position] = keyword.Frequency
			}
			continue
		}

		// 当索引不存在时，插入新索引项
		switch indexer.initOptions.IndexType {
		case LocationsIndex:
			indices.locations = append(indices.locations, []int{})
			copy(indices.locations[position+1:], indices.locations[position:])
			indices.locations[position] = keyword.Starts
		case FrequenciesIndex:
			indices.frequencies = append(indices.frequencies, float32(0))
			copy(indices.frequencies[position+1:], indices.frequencies[position:])
			indices.frequencies[position] = keyword.Frequency
		}
		indices.docIds = append(indices.docIds, 0)
		copy(indices.docIds[position+1:], indices.docIds[position:])
		indices.docIds[position] = document.DocId
	}

	// 更新文章总数
	if docIdIsNew {
		indexer.numDocuments++
	}
}

// 查找包含全部搜索键(AND操作)的文档
// 当docIds不为nil时仅从docIds指定的文档中查找
func (indexer *Indexer) Lookup(
	tokens []string, labels []string, docIds *map[uint64]bool) (docs []IndexedDocument) {
	if indexer.initialized == false {
		log.Fatal("索引器尚未初始化")
	}

	if indexer.numDocuments == 0 {
		return
	}

	// 合并关键词和标签为搜索键
	keywords := make([]string, len(tokens)+len(labels))
	copy(keywords, tokens)
	copy(keywords[len(tokens):], labels)

	indexer.tableLock.RLock()
	defer indexer.tableLock.RUnlock()
	table := make([]*KeywordIndices, len(keywords))
	for i, keyword := range keywords {
		indices, found := indexer.tableLock.table[keyword]
		if !found {
			// 当反向索引表中无此搜索键时直接返回
			return
		} else {
			// 否则加入反向表中
			table[i] = indices
		}
	}

	// 当没有找到时直接返回
	if len(table) == 0 {
		return
	}

	// 归并查找各个搜索键出现文档的交集
	// 从后向前查保证先输出DocId较大文档
	indexPointers := make([]int, len(table))
	for iTable := 0; iTable < len(table); iTable++ {
		indexPointers[iTable] = indexer.getIndexLength(table[iTable]) - 1
	}
	// 平均文本关键词长度，用于计算BM25
	avgDocLength := indexer.totalTokenLength / float32(indexer.numDocuments)
	for ; indexPointers[0] >= 0; indexPointers[0]-- {
		// 以第一个搜索键出现的文档作为基准，并遍历其他搜索键搜索同一文档
		baseDocId := indexer.getDocId(table[0], indexPointers[0])

		if docIds != nil {
			_, found := (*docIds)[baseDocId]
			if !found {
				continue
			}
		}
		iTable := 1
		found := true
		for ; iTable < len(table); iTable++ {
			// 二分法比简单的顺序归并效率高，也有更高效率的算法，
			// 但顺序归并也许是更好的选择，考虑到将来需要用链表重新实现
			// 以避免反向表添加新文档时的写锁。
			// TODO: 进一步研究不同求交集算法的速度和可扩展性。
			position, foundBaseDocId := indexer.searchIndex(table[iTable],
				0, indexPointers[iTable], baseDocId)
			if foundBaseDocId {
				indexPointers[iTable] = position
			} else {
				if position == 0 {
					// 该搜索键中所有的文档ID都比baseDocId大，因此已经没有
					// 继续查找的必要。
					return
				} else {
					// 继续下一indexPointers[0]的查找
					indexPointers[iTable] = position - 1
					found = false
					break
				}
			}
		}

		if found {
			indexedDoc := IndexedDocument{}

			// 当为LocationsIndex时计算关键词紧邻距离
			if indexer.initOptions.IndexType == LocationsIndex {
				// 计算有多少关键词是带有距离信息的
				numTokensWithLocations := 0
				for i, t := range table[:len(tokens)] {
					if len(t.locations[indexPointers[i]]) > 0 {
						numTokensWithLocations++
					}
				}
				if numTokensWithLocations != len(tokens) {
					docs = append(docs, IndexedDocument{
						DocId: baseDocId,
					})
					break
				}

				// 计算搜索键在文档中的紧邻距离
				tokenProximity, tokenLocations := computeTokenProximity(table[:len(tokens)], indexPointers, tokens)
				indexedDoc.TokenProximity = int32(tokenProximity)
				indexedDoc.TokenSnippetLocations = tokenLocations

				// 添加TokenLocations
				indexedDoc.TokenLocations = make([][]int, len(tokens))
				for i, t := range table[:len(tokens)] {
					indexedDoc.TokenLocations[i] = t.locations[indexPointers[i]]
				}
			}

			// 当为LocationsIndex或者FrequenciesIndex时计算BM25
			if indexer.initOptions.IndexType == LocationsIndex ||
				indexer.initOptions.IndexType == FrequenciesIndex {
				bm25 := float32(0)
				d := indexer.docTokenLengths[baseDocId]
				for i, t := range table[:len(tokens)] {
					var frequency float32
					if indexer.initOptions.IndexType == LocationsIndex {
						frequency = float32(len(t.locations[indexPointers[i]]))
					} else {
						frequency = t.frequencies[indexPointers[i]]
					}

					// 计算BM25
					if len(t.docIds) > 0 && frequency > 0 && indexer.initOptions.BM25Parameters != nil && avgDocLength != 0 {
						// 带平滑的idf
						idf := float32(math.Log2(float64(indexer.numDocuments)/float64(len(t.docIds)) + 1))
						k1 := indexer.initOptions.BM25Parameters.K1
						b := indexer.initOptions.BM25Parameters.B
						bm25 += idf * frequency * (k1 + 1) / (frequency + k1*(1-b+b*d/avgDocLength))
					}
				}
				indexedDoc.BM25 = float32(bm25)
			}

			indexedDoc.DocId = baseDocId
			docs = append(docs, indexedDoc)
		}
	}
	return
}

// 二分法查找indices中某文档的索引项
// 第一个返回参数为找到的位置或需要插入的位置
// 第二个返回参数标明是否找到
func (indexer *Indexer) searchIndex(
	indices *KeywordIndices, start int, end int, docId uint64) (int, bool) {
	// 特殊情况
	if indexer.getIndexLength(indices) == start {
		return start, false
	}
	if docId < indexer.getDocId(indices, start) {
		return start, false
	} else if docId == indexer.getDocId(indices, start) {
		return start, true
	}
	if docId > indexer.getDocId(indices, end) {
		return end + 1, false
	} else if docId == indexer.getDocId(indices, end) {
		return end, true
	}

	// 二分
	var middle int
	for end-start > 1 {
		middle = (start + end) / 2
		if docId == indexer.getDocId(indices, middle) {
			return middle, true
		} else if docId > indexer.getDocId(indices, middle) {
			start = middle
		} else {
			end = middle
		}
	}
	return end, false
}

// 计算搜索键在文本中的紧邻距离
//
// 假定第 i 个搜索键首字节出现在文本中的位置为 P_i，长度 L_i
// 紧邻距离计算公式为
//
// 	ArgMin(Sum(Abs(P_(i+1) - P_i - L_i)))
//
// 具体由动态规划实现，依次计算前 i 个 token 在每个出现位置的最优值。
// 选定的 P_i 通过 tokenLocations 参数传回。
func computeTokenProximity(table []*KeywordIndices, indexPointers []int, tokens []string) (
	minTokenProximity int, tokenLocations []int) {
	minTokenProximity = -1
	tokenLocations = make([]int, len(tokens))

	var (
		currentLocations, nextLocations []int
		currentMinValues, nextMinValues []int
		path                            [][]int
	)

	// 初始化路径数组
	path = make([][]int, len(tokens))
	for i := 1; i < len(path); i++ {
		path[i] = make([]int, len(table[i].locations[indexPointers[i]]))
	}

	// 动态规划
	currentLocations = table[0].locations[indexPointers[0]]
	currentMinValues = make([]int, len(currentLocations))
	for i := 1; i < len(tokens); i++ {
		nextLocations = table[i].locations[indexPointers[i]]
		nextMinValues = make([]int, len(nextLocations))
		for j, _ := range nextMinValues {
			nextMinValues[j] = -1
		}

		var iNext int
		for iCurrent, currentLocation := range currentLocations {
			if currentMinValues[iCurrent] == -1 {
				continue
			}
			for iNext+1 < len(nextLocations) && nextLocations[iNext+1] < currentLocation {
				iNext++
			}

			update := func(from int, to int) {
				if to >= len(nextLocations) {
					return
				}
				value := currentMinValues[from] + AbsInt(nextLocations[to]-currentLocations[from]-len(tokens[i-1]))
				if nextMinValues[to] == -1 || value < nextMinValues[to] {
					nextMinValues[to] = value
					path[i][to] = from
				}
			}

			// 最优解的状态转移只发生在左右最接近的位置
			update(iCurrent, iNext)
			update(iCurrent, iNext+1)
		}

		currentLocations = nextLocations
		currentMinValues = nextMinValues
	}

	// 找出最优解
	var cursor int
	for i, value := range currentMinValues {
		if value == -1 {
			continue
		}
		if minTokenProximity == -1 || value < minTokenProximity {
			minTokenProximity = value
			cursor = i
		}
	}

	// 从路径倒推出最优解的位置
	for i := len(tokens) - 1; i >= 0; i-- {
		if i != len(tokens)-1 {
			cursor = path[i+1][cursor]
		}
		tokenLocations[i] = table[i].locations[indexPointers[i]][cursor]
	}
	return
}

// 从KeywordIndices中得到第i个文档的DocId
func (indexer *Indexer) getDocId(ti *KeywordIndices, i int) uint64 {
	return ti.docIds[i]
}

// 得到KeywordIndices中文档总数
func (indexer *Indexer) getIndexLength(ti *KeywordIndices) int {
	return len(ti.docIds)
}

type Ranker struct {
	lock struct {
		sync.RWMutex
		fields map[uint64]interface{}
	}
	initialized bool
}

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
		start = MinInt(options.OutputOffset, len(outputDocs))
		end = MinInt(options.OutputOffset+options.MaxOutputs, len(outputDocs))
	} else {
		start = MinInt(options.OutputOffset, len(outputDocs))
		end = len(outputDocs)
	}
	return outputDocs[start:end]
}

type indexerAddDocumentRequest struct {
	document *DocumentIndex
}

type indexerLookupRequest struct {
	tokens              []string
	labels              []string
	docIds              []uint64
	options             RankOptions
	rankerReturnChannel chan rankerReturnRequest
}

type rankerAddScoringFieldsRequest struct {
	docId  uint64
	fields interface{}
}

type rankerRankRequest struct {
	docs                []IndexedDocument
	options             RankOptions
	rankerReturnChannel chan rankerReturnRequest
}

type rankerReturnRequest struct {
	docs ScoredDocuments
}

type rankerRemoveScoringFieldsRequest struct {
	docId uint64
}

type persistentStorageIndexDocumentRequest struct {
	docId uint64
	data  DocumentIndexData
}

type Engine struct {
	// 计数器，用来统计有多少文档被索引等信息
	numDocumentsIndexed uint64
	numIndexingRequests uint64
	numTokenIndexAdded  uint64
	numDocumentsStored  uint64

	// 记录初始化参数
	initOptions EngineInitOptions
	initialized bool

	indexers   []Indexer
	rankers    []Ranker
	segmenter  cut.Segmenter
	stopTokens StopTokens
	dbs        []*kv.DB

	// 建立索引器使用的通信通道
	segmenterChannel               chan segmenterRequest
	indexerAddDocumentChannels     []chan indexerAddDocumentRequest
	rankerAddScoringFieldsChannels []chan rankerAddScoringFieldsRequest

	// 建立排序器使用的通信通道
	indexerLookupChannels             []chan indexerLookupRequest
	rankerRankChannels                []chan rankerRankRequest
	rankerRemoveScoringFieldsChannels []chan rankerRemoveScoringFieldsRequest

	// 建立持久存储使用的通信通道
	persistentStorageIndexDocumentChannels []chan persistentStorageIndexDocumentRequest
	persistentStorageInitChannel           chan bool
}

func (engine *Engine) Init(options EngineInitOptions) {
	// 将线程数设置为CPU数
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 初始化初始参数
	if engine.initialized {
		log.Fatal("请勿重复初始化引擎")
	}
	options.Init()
	engine.initOptions = options
	engine.initialized = true

	// 载入分词器词典
	//engine.segmenter.LoadDictionary(options.SegmenterDictionaries)
	//将词典载入单独分离出来
	engine.segmenter = options.Segmenter

	// 初始化停用词
	engine.stopTokens.Init(options.StopTokenFile)

	// 初始化索引器和排序器
	for shard := 0; shard < options.NumShards; shard++ {
		engine.indexers = append(engine.indexers, Indexer{})
		engine.indexers[shard].Init(*options.IndexerInitOptions)

		engine.rankers = append(engine.rankers, Ranker{})
		engine.rankers[shard].Init()
	}

	// 初始化分词器通道
	engine.segmenterChannel = make(
		chan segmenterRequest, options.NumSegmenterThreads)

	// 初始化索引器通道
	engine.indexerAddDocumentChannels = make(
		[]chan indexerAddDocumentRequest, options.NumShards)
	engine.indexerLookupChannels = make(
		[]chan indexerLookupRequest, options.NumShards)
	for shard := 0; shard < options.NumShards; shard++ {
		engine.indexerAddDocumentChannels[shard] = make(
			chan indexerAddDocumentRequest,
			options.IndexerBufferLength)
		engine.indexerLookupChannels[shard] = make(
			chan indexerLookupRequest,
			options.IndexerBufferLength)
	}

	// 初始化排序器通道
	engine.rankerAddScoringFieldsChannels = make(
		[]chan rankerAddScoringFieldsRequest, options.NumShards)
	engine.rankerRankChannels = make(
		[]chan rankerRankRequest, options.NumShards)
	engine.rankerRemoveScoringFieldsChannels = make(
		[]chan rankerRemoveScoringFieldsRequest, options.NumShards)
	for shard := 0; shard < options.NumShards; shard++ {
		engine.rankerAddScoringFieldsChannels[shard] = make(
			chan rankerAddScoringFieldsRequest,
			options.RankerBufferLength)
		engine.rankerRankChannels[shard] = make(
			chan rankerRankRequest,
			options.RankerBufferLength)
		engine.rankerRemoveScoringFieldsChannels[shard] = make(
			chan rankerRemoveScoringFieldsRequest,
			options.RankerBufferLength)
	}

	// 初始化持久化存储通道
	if engine.initOptions.UsePersistentStorage {
		engine.persistentStorageIndexDocumentChannels =
			make([]chan persistentStorageIndexDocumentRequest,
				engine.initOptions.PersistentStorageShards)
		for shard := 0; shard < engine.initOptions.PersistentStorageShards; shard++ {
			engine.persistentStorageIndexDocumentChannels[shard] = make(
				chan persistentStorageIndexDocumentRequest)
		}
		engine.persistentStorageInitChannel = make(
			chan bool, engine.initOptions.PersistentStorageShards)
	}

	// 启动分词器
	for iThread := 0; iThread < options.NumSegmenterThreads; iThread++ {
		go engine.segmenterWorker()
	}

	// 启动索引器和排序器
	for shard := 0; shard < options.NumShards; shard++ {
		go engine.indexerAddDocumentWorker(shard)
		go engine.rankerAddScoringFieldsWorker(shard)
		go engine.rankerRemoveScoringFieldsWorker(shard)

		for i := 0; i < options.NumIndexerThreadsPerShard; i++ {
			go engine.indexerLookupWorker(shard)
		}
		for i := 0; i < options.NumRankerThreadsPerShard; i++ {
			go engine.rankerRankWorker(shard)
		}
	}

	// 启动持久化存储工作协程
	if engine.initOptions.UsePersistentStorage {
		err := os.MkdirAll(engine.initOptions.PersistentStorageFolder, 0700)
		if err != nil {
			log.Fatal("无法创建目录", engine.initOptions.PersistentStorageFolder)
		}

		// 打开或者创建数据库
		engine.dbs = make([]*kv.DB, engine.initOptions.PersistentStorageShards)
		for shard := 0; shard < engine.initOptions.PersistentStorageShards; shard++ {
			dbPath := engine.initOptions.PersistentStorageFolder + "/" + PersistentStorageFilePrefix + "." + strconv.Itoa(shard)
			db, err := OpenOrCreateKv(dbPath, &kv.Options{})
			if db == nil || err != nil {
				log.Fatal("无法打开数据库", dbPath, ": ", err, db)
			}
			engine.dbs[shard] = db
		}

		// 从数据库中恢复
		for shard := 0; shard < engine.initOptions.PersistentStorageShards; shard++ {
			go engine.persistentStorageInitWorker(shard)
		}

		// 等待恢复完成
		for shard := 0; shard < engine.initOptions.PersistentStorageShards; shard++ {
			<-engine.persistentStorageInitChannel
		}
		for {
			runtime.Gosched()
			if engine.numIndexingRequests == engine.numDocumentsIndexed {
				break
			}
		}

		// 关闭并重新打开数据库
		for shard := 0; shard < engine.initOptions.PersistentStorageShards; shard++ {
			engine.dbs[shard].Close()
			dbPath := engine.initOptions.PersistentStorageFolder + "/" + PersistentStorageFilePrefix + "." + strconv.Itoa(shard)
			db, err := OpenOrCreateKv(dbPath, &kv.Options{})
			if db == nil || err != nil {
				log.Fatal("无法打开数据库", dbPath, ": ", err)
			}
			engine.dbs[shard] = db
		}

		for shard := 0; shard < engine.initOptions.PersistentStorageShards; shard++ {
			go engine.persistentStorageIndexDocumentWorker(shard)
		}
	}

	atomic.AddUint64(&engine.numDocumentsStored, engine.numIndexingRequests)
}

func (engine *Engine) rankerAddScoringFieldsWorker(shard int) {
	for {
		request := <-engine.rankerAddScoringFieldsChannels[shard]
		engine.rankers[shard].AddScoringFields(request.docId, request.fields)
	}
}

func (engine *Engine) rankerRankWorker(shard int) {
	for {
		request := <-engine.rankerRankChannels[shard]
		if request.options.MaxOutputs != 0 {
			request.options.MaxOutputs += request.options.OutputOffset
		}
		request.options.OutputOffset = 0
		outputDocs := engine.rankers[shard].Rank(request.docs, request.options)
		request.rankerReturnChannel <- rankerReturnRequest{docs: outputDocs}
	}
}

func (engine *Engine) rankerRemoveScoringFieldsWorker(shard int) {
	for {
		request := <-engine.rankerRemoveScoringFieldsChannels[shard]
		engine.rankers[shard].RemoveScoringFields(request.docId)
	}
}

// 将文档加入索引
//
// 输入参数：
// 	docId	标识文档编号，必须唯一
//	data	见DocumentIndexData注释
//
// 注意：
//      1. 这个函数是线程安全的，请尽可能并发调用以提高索引速度
// 	2. 这个函数调用是非同步的，也就是说在函数返回时有可能文档还没有加入索引中，因此
//         如果立刻调用Search可能无法查询到这个文档。强制刷新索引请调用FlushIndex函数。
func (engine *Engine) IndexDocument(docId uint64, data DocumentIndexData) {
	engine.internalIndexDocument(docId, data)

	hash := Murmur3([]byte(fmt.Sprint("%d", docId))) % uint32(engine.initOptions.PersistentStorageShards)
	if engine.initOptions.UsePersistentStorage {
		engine.persistentStorageIndexDocumentChannels[hash] <- persistentStorageIndexDocumentRequest{docId: docId, data: data}
	}
}

func (engine *Engine) internalIndexDocument(docId uint64, data DocumentIndexData) {
	if !engine.initialized {
		log.Fatal("必须先初始化引擎")
	}

	atomic.AddUint64(&engine.numIndexingRequests, 1)
	hash := Murmur3([]byte(fmt.Sprint("%d%s", docId, data.Content)))
	engine.segmenterChannel <- segmenterRequest{
		docId: docId, hash: hash, data: data}
}

// 将文档从索引中删除
//
// 输入参数：
// 	docId	标识文档编号，必须唯一
//
// 注意：这个函数仅从排序器中删除文档的自定义评分字段，索引器不会发生变化。所以
// 你的自定义评分字段必须能够区别评分字段为nil的情况，并将其从排序结果中删除。
func (engine *Engine) RemoveDocument(docId uint64) {
	if !engine.initialized {
		log.Fatal("必须先初始化引擎")
	}

	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.rankerRemoveScoringFieldsChannels[shard] <- rankerRemoveScoringFieldsRequest{docId: docId}
	}

	if engine.initOptions.UsePersistentStorage {
		// 从数据库中删除
		hash := Murmur3([]byte(fmt.Sprint("%d", docId))) % uint32(engine.initOptions.PersistentStorageShards)
		go engine.persistentStorageRemoveDocumentWorker(docId, hash)
	}
}

// 阻塞等待直到所有索引添加完毕
func (engine *Engine) FlushIndex() {
	for {
		runtime.Gosched()
		if engine.numIndexingRequests == engine.numDocumentsIndexed &&
			(!engine.initOptions.UsePersistentStorage ||
				engine.numIndexingRequests == engine.numDocumentsStored) {
			return
		}
	}
}

func (engine *Engine) segmenterWorker() {
	for {
		request := <-engine.segmenterChannel
		shard := engine.getShard(request.hash)

		tokensMap := make(map[string][]int)
		numTokens := 0
		if request.data.Content != "" {
			// 当文档正文不为空时，优先从内容分词中得到关键词
			segments := engine.segmenter.Cut([]byte(request.data.Content), true)
			for _, segment := range segments {
				token := segment.Token().Text()
				if !engine.stopTokens.IsStopToken(token) {
					tokensMap[token] = append(tokensMap[token], segment.Start())
				}
			}
			numTokens = len(segments)
		} else {
			// 否则载入用户输入的关键词
			for _, t := range request.data.Tokens {
				if !engine.stopTokens.IsStopToken(t.Text) {
					tokensMap[t.Text] = t.Locations
				}
			}
			numTokens = len(request.data.Tokens)
		}

		// 加入非分词的文档标签
		for _, label := range request.data.Labels {
			if !engine.stopTokens.IsStopToken(label) {
				tokensMap[label] = []int{}
			}
		}

		indexerRequest := indexerAddDocumentRequest{
			document: &DocumentIndex{
				DocId:       request.docId,
				TokenLength: float32(numTokens),
				Keywords:    make([]KeywordIndex, len(tokensMap)),
			},
		}
		iTokens := 0
		for k, v := range tokensMap {
			indexerRequest.document.Keywords[iTokens] = KeywordIndex{
				Text: k,
				// 非分词标注的词频设置为0，不参与tf-idf计算
				Frequency: float32(len(v)),
				Starts:    v}
			iTokens++
		}
		engine.indexerAddDocumentChannels[shard] <- indexerRequest
		rankerRequest := rankerAddScoringFieldsRequest{
			docId: request.docId, fields: request.data.Fields}
		engine.rankerAddScoringFieldsChannels[shard] <- rankerRequest
	}
}

// 查找满足搜索条件的文档，此函数线程安全
func (engine *Engine) Search(request SearchRequest) (output SearchResponse) {
	if !engine.initialized {
		log.Fatal("必须先初始化引擎")
	}

	var rankOptions RankOptions
	if request.RankOptions == nil {
		rankOptions = *engine.initOptions.DefaultRankOptions
	} else {
		rankOptions = *request.RankOptions
	}
	if rankOptions.ScoringCriteria == nil {
		rankOptions.ScoringCriteria = engine.initOptions.DefaultRankOptions.ScoringCriteria
	}

	// 收集关键词
	tokens := []string{}
	if request.Text != "" {
		querySegments := engine.segmenter.Cut([]byte(request.Text), true)
		for _, s := range querySegments {
			token := s.Token().Text()
			if !engine.stopTokens.IsStopToken(token) {
				tokens = append(tokens, s.Token().Text())
			}
		}
	} else {
		for _, t := range request.Tokens {
			tokens = append(tokens, t)
		}
	}

	// 建立排序器返回的通信通道
	rankerReturnChannel := make(
		chan rankerReturnRequest, engine.initOptions.NumShards)

	// 生成查找请求
	lookupRequest := indexerLookupRequest{
		tokens:              tokens,
		labels:              request.Labels,
		docIds:              request.DocIds,
		options:             rankOptions,
		rankerReturnChannel: rankerReturnChannel}

	// 向索引器发送查找请求
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.indexerLookupChannels[shard] <- lookupRequest
	}

	// 从通信通道读取排序器的输出
	rankOutput := ScoredDocuments{}
	timeout := request.Timeout
	isTimeout := false
	if timeout <= 0 {
		// 不设置超时
		for shard := 0; shard < engine.initOptions.NumShards; shard++ {
			rankerOutput := <-rankerReturnChannel
			for _, doc := range rankerOutput.docs {
				rankOutput = append(rankOutput, doc)
			}
		}
	} else {
		// 设置超时
		deadline := time.Now().Add(time.Nanosecond * time.Duration(NumNanosecondsInAMillisecond*request.Timeout))
		for shard := 0; shard < engine.initOptions.NumShards; shard++ {
			select {
			case rankerOutput := <-rankerReturnChannel:
				for _, doc := range rankerOutput.docs {
					rankOutput = append(rankOutput, doc)
				}
			case <-time.After(deadline.Sub(time.Now())):
				isTimeout = true
				break
			}
		}
	}

	// 再排序
	if rankOptions.ReverseOrder {
		sort.Sort(sort.Reverse(rankOutput))
	} else {
		sort.Sort(rankOutput)
	}

	// 准备输出
	output.Tokens = tokens
	var start, end int
	if rankOptions.MaxOutputs == 0 {
		start = MinInt(rankOptions.OutputOffset, len(rankOutput))
		end = len(rankOutput)
	} else {
		start = MinInt(rankOptions.OutputOffset, len(rankOutput))
		end = MinInt(start+rankOptions.MaxOutputs, len(rankOutput))
	}
	output.Docs = rankOutput[start:end]
	output.Timeout = isTimeout
	return
}

func (engine *Engine) indexerAddDocumentWorker(shard int) {
	for {
		request := <-engine.indexerAddDocumentChannels[shard]
		engine.indexers[shard].AddDocument(request.document)
		atomic.AddUint64(&engine.numTokenIndexAdded,
			uint64(len(request.document.Keywords)))
		atomic.AddUint64(&engine.numDocumentsIndexed, 1)
	}
}

func (engine *Engine) indexerLookupWorker(shard int) {
	for {
		request := <-engine.indexerLookupChannels[shard]

		var docs []IndexedDocument
		if len(request.docIds) == 0 {
			docs = engine.indexers[shard].Lookup(request.tokens, request.labels, nil)
		} else {
			docIds := make(map[uint64]bool)
			//通过request.docIds 生成查询字典
			if len(request.docIds) != 2 {
				continue
			}
			for i := request.docIds[0]; i <= request.docIds[1]; i++ {
				docIds[i] = true
			}
			/*
				for _, ids := range request.docIds {
					docIds[ids] = true
				}
			*/
			docs = engine.indexers[shard].Lookup(request.tokens, request.labels, &docIds)
		}

		if len(docs) == 0 {
			request.rankerReturnChannel <- rankerReturnRequest{}
			continue
		}

		rankerRequest := rankerRankRequest{
			docs:                docs,
			options:             request.options,
			rankerReturnChannel: request.rankerReturnChannel}
		engine.rankerRankChannels[shard] <- rankerRequest
	}
}

func (engine *Engine) persistentStorageIndexDocumentWorker(shard int) {
	for {
		request := <-engine.persistentStorageIndexDocumentChannels[shard]

		// 得到key
		b := make([]byte, 10)
		length := binary.PutUvarint(b, request.docId)

		// 得到value
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(request.data)
		if err != nil {
			atomic.AddUint64(&engine.numDocumentsStored, 1)
			continue
		}

		// 将key-value写入数据库
		engine.dbs[shard].Set(b[0:length], buf.Bytes())
		atomic.AddUint64(&engine.numDocumentsStored, 1)
	}
}

func (engine *Engine) persistentStorageRemoveDocumentWorker(docId uint64, shard uint32) {
	// 得到key
	b := make([]byte, 10)
	length := binary.PutUvarint(b, docId)

	// 从数据库删除该key
	engine.dbs[shard].Delete(b[0:length])
}

func (engine *Engine) persistentStorageInitWorker(shard int) {
	iter, err := engine.dbs[shard].SeekFirst()
	if err == io.EOF {
		engine.persistentStorageInitChannel <- true
		return
	} else if err != nil {
		engine.persistentStorageInitChannel <- true
		log.Fatal("无法遍历数据库")
	}

	for {
		key, value, err := iter.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			continue
		}

		// 得到docID
		docId, _ := binary.Uvarint(key)

		// 得到data
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		var data DocumentIndexData
		err = dec.Decode(&data)
		if err != nil {
			continue
		}

		// 添加索引
		engine.internalIndexDocument(docId, data)
	}
	engine.persistentStorageInitChannel <- true
}

func (engine *Engine) NumTokenIndexAdded() uint64 {
	return engine.numTokenIndexAdded
}

func (engine *Engine) NumDocumentsIndexed() uint64 {
	return engine.numDocumentsIndexed
}

// 关闭引擎
func (engine *Engine) Close() {
	engine.FlushIndex()
	if engine.initOptions.UsePersistentStorage {
		for _, db := range engine.dbs {
			db.Close()
		}
	}
}

// 从文本hash得到要分配到的shard
func (engine *Engine) getShard(hash uint32) int {
	return int(hash - hash/uint32(engine.initOptions.NumShards)*uint32(engine.initOptions.NumShards))
}

func AbsInt(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 打开或者创建KV数据库
// 当path指向的数据库存在时打开该数据库，
//否则尝试在该路径处创建新数据库
func OpenOrCreateKv(path string, options *kv.Options) (*kv.DB, error) {
	db, errOpen := kv.Open(path, options)
	if errOpen != nil {
		var errCreate error
		db, errCreate = kv.Create(path, options)
		if errCreate != nil {
			return db, errCreate
		}
	}

	return db, nil
}

//MurmurHash算法：高运算性能，低碰撞率的hash算法
const (
	c1 = 0xcc9e2d51
	c2 = 0x1b873593
	c3 = 0x85ebca6b
	c4 = 0xc2b2ae35
	r1 = 15
	r2 = 13
	m  = 5
	n  = 0xe6546b64
)

var (
	Seed = uint32(1)
)

func Murmur3(key []byte) (hash uint32) {
	hash = Seed
	iByte := 0
	for ; iByte+4 <= len(key); iByte += 4 {
		k := uint32(key[iByte]) | uint32(key[iByte+1])<<8 | uint32(key[iByte+2])<<16 | uint32(key[iByte+3])<<24
		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2
		hash ^= k
		hash = (hash << r2) | (hash >> (32 - r2))
		hash = hash*m + n
	}

	var remainingBytes uint32
	switch len(key) - iByte {
	case 3:
		remainingBytes += uint32(key[iByte+2]) << 16
		fallthrough
	case 2:
		remainingBytes += uint32(key[iByte+1]) << 8
		fallthrough
	case 1:
		remainingBytes += uint32(key[iByte])
		remainingBytes *= c1
		remainingBytes = (remainingBytes << r1) | (remainingBytes >> (32 - r1))
		remainingBytes = remainingBytes * c2
		hash ^= remainingBytes
	}

	hash ^= uint32(len(key))
	hash ^= hash >> 16
	hash *= c3
	hash ^= hash >> 13
	hash *= c4
	hash ^= hash >> 16

	// 出发吧，狗嬷嬷！
	return
}
