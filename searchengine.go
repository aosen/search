package search

//搜索引擎的基类，所有定制化搜索引擎继承此类
//继承 Engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/aosen/search/utils"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"sync/atomic"
	"time"
)

const (
	NumNanosecondsInAMillisecond = 1000000
)

// 文档的一个关键词
type TokenData struct {
	// 关键词的字符串
	Text string

	// 关键词的首字节在文档中出现的位置
	Locations []int
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
	for iScore := 0; iScore < utils.MinInt(len(docs[i].Scores), len(docs[j].Scores)); iScore++ {
		if docs[i].Scores[iScore] > docs[j].Scores[iScore] {
			return true
		} else if docs[i].Scores[iScore] < docs[j].Scores[iScore] {
			return false
		}
	}
	return len(docs[i].Scores) > len(docs[j].Scores)
}

type segmenterRequest struct {
	docId uint64
	hash  uint32
	data  DocumentIndexData
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

type EngineInitOptions struct {
	// 半角逗号分隔的字典文件，具体用法见
	// sego.Segmenter.LoadDictionary函数的注释
	Segmenter SearchSegmenter

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
	UsePersistentStorage bool

	//索引存储接口对接
	SearchPipline SearchPipline
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
)

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
}

// 搜索引擎基类
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
	segmenter  SearchSegmenter
	stopTokens StopTokens
	//dbs        []*kv.DB
	searchpipline SearchPipline

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
	if engine.initOptions.UsePersistentStorage && engine.initOptions.SearchPipline != nil {
		storageshards := engine.initOptions.SearchPipline.GetStorageShards()
		engine.persistentStorageIndexDocumentChannels =
			make([]chan persistentStorageIndexDocumentRequest,
				storageshards)
		for shard := 0; shard < storageshards; shard++ {
			engine.persistentStorageIndexDocumentChannels[shard] = make(
				chan persistentStorageIndexDocumentRequest)
		}
		engine.persistentStorageInitChannel = make(
			chan bool, storageshards)
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
		engine.searchpipline = options.SearchPipline
		engine.searchpipline.Init()

		storageshards := engine.searchpipline.GetStorageShards()
		// 从数据库中恢复
		for shard := 0; shard < storageshards; shard++ {
			go engine.persistentStorageInitWorker(shard)
		}

		// 等待恢复完成
		for shard := 0; shard < storageshards; shard++ {
			<-engine.persistentStorageInitChannel
		}
		for {
			runtime.Gosched()
			if engine.numIndexingRequests == engine.numDocumentsIndexed {
				break
			}
		}

		// 关闭并重新打开数据库
		for shard := 0; shard < storageshards; shard++ {
			engine.searchpipline.Close(shard)
			engine.searchpipline.Conn(shard)
		}

		for shard := 0; shard < storageshards; shard++ {
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

	if engine.initOptions.UsePersistentStorage {
		hash := utils.Murmur3([]byte(fmt.Sprint("%d", docId))) % uint32(engine.searchpipline.GetStorageShards())
		engine.persistentStorageIndexDocumentChannels[hash] <- persistentStorageIndexDocumentRequest{docId: docId, data: data}
	}
}

func (engine *Engine) internalIndexDocument(docId uint64, data DocumentIndexData) {
	if !engine.initialized {
		log.Fatal("必须先初始化引擎")
	}

	atomic.AddUint64(&engine.numIndexingRequests, 1)
	hash := utils.Murmur3([]byte(fmt.Sprint("%d%s", docId, data.Content)))
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
		hash := utils.Murmur3([]byte(fmt.Sprint("%d", docId))) % uint32(engine.searchpipline.GetStorageShards())
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
				token := segment.GetToken().GetText()
				if !engine.stopTokens.IsStopToken(token) {
					tokensMap[token] = append(tokensMap[token], segment.GetStart())
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
			token := s.GetToken().GetText()
			if !engine.stopTokens.IsStopToken(token) {
				tokens = append(tokens, s.GetToken().GetText())
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
		start = utils.MinInt(rankOptions.OutputOffset, len(rankOutput))
		end = len(rankOutput)
	} else {
		start = utils.MinInt(rankOptions.OutputOffset, len(rankOutput))
		end = utils.MinInt(start+rankOptions.MaxOutputs, len(rankOutput))
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
			//通过request.docIds 生成查询字典
			if (len(request.docIds) != 2) || (request.docIds[0] > request.docIds[1]) {
				continue
			}
			/*
				docIds := make(map[uint64]bool, request.docIds[1]-request.docIds[0]+1)
				//这个过程比较浪费时间
				log.Println("map", shard, time.Now().UnixNano())
				for i := request.docIds[0]; i <= request.docIds[1]; i++ {
					docIds[i] = true
				}
				log.Println("map", shard, time.Now().UnixNano())
			*/
			/*
				for _, ids := range request.docIds {
					docIds[ids] = true
				}
			*/
			//将上方代码注释，此处无需生成字典，继续传递docids的范围
			//就行，然后只要判断最终搜索出来的结果在不在这个范围内就OK
			/*
				docs = engine.indexers[shard].Lookup(request.tokens, request.labels, &docIds)
			*/
			docs = engine.indexers[shard].Lookup(request.tokens, request.labels, request.docIds)
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
		engine.searchpipline.Set(shard, b[0:length], buf.Bytes())
		atomic.AddUint64(&engine.numDocumentsStored, 1)
	}
}

func (engine *Engine) persistentStorageRemoveDocumentWorker(docId uint64, shard uint32) {
	// 得到key
	b := make([]byte, 10)
	length := binary.PutUvarint(b, docId)

	s := int(shard)
	// 从数据库删除该key
	engine.searchpipline.Delete(s, b[0:length])
}

func (engine *Engine) persistentStorageInitWorker(shard int) {
	err := engine.searchpipline.Recover(shard, engine.internalIndexDocument)
	if err == io.EOF {
		engine.persistentStorageInitChannel <- true
		return
	} else if err != nil {
		engine.persistentStorageInitChannel <- true
		log.Fatal("无法遍历数据库")
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
		storageshards := engine.searchpipline.GetStorageShards()
		for shard := 0; shard < storageshards; shard++ {
			engine.searchpipline.Close(shard)
		}
	}
}

// 从文本hash得到要分配到的shard
func (engine *Engine) getShard(hash uint32) int {
	return int(hash - hash/uint32(engine.initOptions.NumShards)*uint32(engine.initOptions.NumShards))
}

//停用词管理
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
