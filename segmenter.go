package search

//中文分词的基类
//Segmenter

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	minTokenFrequency = 2 // 仅从字典文件中读取大于等于此频率的分词
)

// 字串类型，可以用来表达
//	1. 一个字元，比如"中"又如"国", 英文的一个字元是一个词
//	2. 一个分词，比如"中国"又如"人口"
//	3. 一段文字，比如"中国有十三亿人口"
type Text []byte

// 文本中的一个分词
type Segment struct {
	// 分词在文本中的起始字节位置
	start int
	// 分词在文本中的结束字节位置（不包括该位置）
	end int
	// 分词信息
	token *Token
}

// 返回分词在文本中的起始字节位置
func (self *Segment) Start() int {
	return self.start
}

// 返回分词在文本中的结束字节位置（不包括该位置）
func (self *Segment) End() int {
	return self.end
}

// 返回分词信息
func (self *Segment) Token() *Token {
	return self.token
}

// 一个分词
type Token struct {
	// 分词的字串，这实际上是个字元数组
	text []Text
	// 分词在语料库中的词频
	frequency int
	// log2(总词频/该分词词频)，这相当于log2(1/p(分词))，用作动态规划中
	// 该分词的路径长度。求解prod(p(分词))的最大值相当于求解
	// sum(distance(分词))的最小值，这就是“最短路径”的来历。
	distance float32
	// 词性标注
	pos string
	// 该分词文本的进一步分词划分，见Segments函数注释。
	segments []*Segment
}

// 返回分词文本
func (token *Token) Text() string {
	return textSliceToString(token.text)
}

// 返回分词在语料库中的词频
func (token *Token) Frequency() int {
	return token.frequency
}

// 返回分词词性标注
func (token *Token) Pos() string {
	return token.pos
}

// 该分词文本的进一步分词划分，比如"中华人民共和国中央人民政府"这个分词
// 有两个子分词"中华人民共和国"和"中央人民政府"。子分词也可以进一步有子分词
// 形成一个树结构，遍历这个树就可以得到该分词的所有细致分词划分，这主要
// 用于搜索引擎对一段文本进行全文搜索。
func (token *Token) Segments() []*Segment {
	return token.segments
}

/*
Dictionary结构体实现了一个字串前缀树，
一个分词可能出现在叶子节点也有可能出现在非叶节点
*/
type Dictionary struct {
	root           node     // 根节点
	maxTokenLength int      // 词典中最长的分词
	numTokens      int      // 词典中分词数目
	tokens         []*Token // 词典中所有的分词，方便遍历
	totalFrequency int64    // 词典中所有分词的频率之和
}

// 前缀树节点
type node struct {
	word     Text    // 该节点对应的字元
	token    *Token  // 当此节点没有对应的分词时值为nil
	children []*node // 该字元后继的所有可能字元，当为叶子节点时为空
}

// 词典中分词数目
func (self *Dictionary) NumTokens() int {
	return self.numTokens
}

// 词典中所有分词的频率之和
func (self *Dictionary) TotalFrequency() int64 {
	return self.totalFrequency
}

// 向词典中加入一个分词
func (self *Dictionary) addToken(token *Token) {
	current := &self.root
	for _, word := range token.text {
		// 一边向深处移动一边添加节点（如果需要的话）
		current = upsert(&current.children, word)
	}

	// 当这个分词不存在词典中时添加此分词，否则忽略
	if current.token == nil {
		current.token = token
		if len(token.text) > self.maxTokenLength {
			self.maxTokenLength = len(token.text)
		}
		self.numTokens++
		self.tokens = append(self.tokens, token)
		self.totalFrequency += int64(token.frequency)
	}
}

// 在词典中查找和字元组words可以前缀匹配的所有分词
// 返回值为找到的分词数
func (self *Dictionary) lookupTokens(words []Text, tokens []*Token) int {
	// 特殊情况
	if len(words) == 0 {
		return 0
	}

	current := &self.root
	numTokens := 0
	for _, word := range words {
		// 如果已经抵达叶子节点则不再继续寻找
		if len(current.children) == 0 {
			break
		}

		// 否则在该节点子节点中进行下个字元的匹配
		index, found := binarySearch(current.children, word)
		if !found {
			break
		}

		// 匹配成功，则跳入匹配的子节点中
		current = current.children[index]
		if current.token != nil {
			tokens[numTokens] = current.token
			numTokens++
		}
	}
	return numTokens
}

// 词典中最长的分词
func (self *Dictionary) MaxTokenLength() int {
	return self.maxTokenLength
}

//分词器结构体
type Segmenter struct {
	dict *Dictionary
}

// 该结构体用于记录Viterbi算法中某字元处的向前分词跳转信息
type jumper struct {
	minDistance float32
	token       *Token
}

// 返回分词器使用的词典
func (self *Segmenter) Dictionary() *Dictionary {
	return self.dict
}

func (self *Segmenter) segmentWords(text []Text, searchMode bool) []Segment {
	// 搜索模式下该分词已无继续划分可能的情况
	if searchMode && len(text) == 1 {
		return []Segment{}
	}

	// jumpers定义了每个字元处的向前跳转信息，包括这个跳转对应的分词，
	// 以及从文本段开始到该字元的最短路径值
	jumpers := make([]jumper, len(text))

	tokens := make([]*Token, self.dict.maxTokenLength)
	for current := 0; current < len(text); current++ {
		// 找到前一个字元处的最短路径，以便计算后续路径值
		var baseDistance float32
		if current == 0 {
			// 当本字元在文本首部时，基础距离应该是零
			baseDistance = 0
		} else {
			baseDistance = jumpers[current-1].minDistance
		}

		// 寻找所有以当前字元开头的分词
		numTokens := self.dict.lookupTokens(
			text[current:minInt(current+self.dict.maxTokenLength, len(text))], tokens)

		// 对所有可能的分词，更新分词结束字元处的跳转信息
		for iToken := 0; iToken < numTokens; iToken++ {
			location := current + len(tokens[iToken].text) - 1
			if !searchMode || current != 0 || location != len(text)-1 {
				updateJumper(&jumpers[location], baseDistance, tokens[iToken])
			}
		}

		// 当前字元没有对应分词时补加一个伪分词
		if numTokens == 0 || len(tokens[0].text) > 1 {
			updateJumper(&jumpers[current], baseDistance,
				&Token{text: []Text{text[current]}, frequency: 1, distance: 32, pos: "x"})
		}
	}

	// 从后向前扫描第一遍得到需要添加的分词数目
	numSeg := 0
	for index := len(text) - 1; index >= 0; {
		location := index - len(jumpers[index].token.text) + 1
		numSeg++
		index = location - 1
	}

	// 从后向前扫描第二遍添加分词到最终结果
	outputSegments := make([]Segment, numSeg)
	for index := len(text) - 1; index >= 0; {
		location := index - len(jumpers[index].token.text) + 1
		numSeg--
		outputSegments[numSeg].token = jumpers[index].token
		index = location - 1
	}

	// 计算各个分词的字节位置
	bytePosition := 0
	for iSeg := 0; iSeg < len(outputSegments); iSeg++ {
		outputSegments[iSeg].start = bytePosition
		bytePosition += textSliceByteLength(outputSegments[iSeg].token.text)
		outputSegments[iSeg].end = bytePosition
	}
	return outputSegments
}

// 从文件中载入词典
// 可以载入多个词典文件，文件名用","分隔，排在前面的词典优先载入分词，比如
// 	"用户词典.txt,通用词典.txt"
// 当一个分词既出现在用户词典也出现在通用词典中，则优先使用用户词典。
// 词典的格式为（每个分词一行）：
//	分词文本 频率 词性
func (self *Segmenter) LoadDictionary(files string) {
	self.dict = new(Dictionary)
	for _, file := range strings.Split(files, ",") {
		log.Printf("载入 %s 词典", file)
		dictFile, err := os.Open(file)
		defer dictFile.Close()
		if err != nil {
			log.Fatalf("无法载入字典文件 \"%s\" \n", file)
		}

		reader := bufio.NewReader(dictFile)

		//词
		var text string
		//词频字符串
		var freqText string
		//词频int
		var frequency int
		//词性
		var pos string

		// 逐行读入分词
		for {
			size, _ := fmt.Fscanln(reader, &text, &freqText, &pos)
			if size == 0 {
				// 文件结束
				break
			} else if size < 2 {
				// 无效行
				continue
			} else if size == 2 {
				// 没有词性标注时设为空字符串
				pos = ""
			}
			// 解析词频
			var err error
			frequency, err = strconv.Atoi(freqText)
			if err != nil {
				continue
			}
			// 过滤频率太小的词
			if frequency < minTokenFrequency {
				continue
			}

			// 将分词添加到字典中
			words := splitTextToWords([]byte(text))
			token := Token{text: words, frequency: frequency, pos: pos}
			self.dict.addToken(&token)
		}
	}

	// 计算每个分词的路径值，路径值含义见Token结构体的注释
	logTotalFrequency := float32(math.Log2(float64(self.dict.totalFrequency)))
	for _, token := range self.dict.tokens {
		token.distance = logTotalFrequency - float32(math.Log2(float64(token.frequency)))
	}

	// 对每个分词进行细致划分，用于搜索引擎模式，该模式用法见Token结构体的注释。
	for _, token := range self.dict.tokens {
		segments := self.segmentWords(token.text, true)

		// 计算需要添加的子分词数目
		numTokensToAdd := 0
		for iToken := 0; iToken < len(segments); iToken++ {
			if len(segments[iToken].token.text) > 1 {
				// 略去字元长度为一的分词
				// TODO: 这值得进一步推敲，特别是当字典中有英文复合词的时候
				numTokensToAdd++
			}
		}
		token.segments = make([]*Segment, numTokensToAdd)

		// 添加子分词
		iSegmentsToAdd := 0
		for iToken := 0; iToken < len(segments); iToken++ {
			if len(segments[iToken].token.text) > 1 {
				token.segments[iSegmentsToAdd] = &segments[iToken]
				iSegmentsToAdd++
			}
		}
	}

	log.Println("词典载入完毕")
}

// 对文本分词
// 输入参数：
//	bytes	UTF8文本的字节数组
// 输出：
//	[]Segment	划分的分词
func (self *Segmenter) Cut(bytes []byte, model bool) []Segment {
	// 处理特殊情况
	if len(bytes) == 0 {
		return []Segment{}
	}
	// 划分字元
	text := splitTextToWords(bytes)
	return self.segmentWords(text, model)
}

// 将文本划分成字元
func splitTextToWords(text Text) []Text {
	output := make([]Text, 0, len(text)/8)
	current := 0
	inAlphanumeric := true
	alphanumericStart := 0
	for current < len(text) {
		r, size := utf8.DecodeRune(text[current:])
		if size <= 2 && (unicode.IsLetter(r) || unicode.IsNumber(r)) {
			// 当前是拉丁字母或数字（非中日韩文字）
			if !inAlphanumeric {
				alphanumericStart = current
				inAlphanumeric = true
			}
		} else {
			if inAlphanumeric {
				inAlphanumeric = false
				if current != 0 {
					output = append(output, toLower(text[alphanumericStart:current]))
				}
			}
			output = append(output, text[current:current+size])
		}
		current += size
	}

	// 处理最后一个字元是英文的情况
	if inAlphanumeric {
		if current != 0 {
			output = append(output, toLower(text[alphanumericStart:current]))
		}
	}

	return output
}

// 将英文词转化为小写
func toLower(text []byte) []byte {
	output := make([]byte, len(text))
	for i, t := range text {
		if t >= 'A' && t <= 'Z' {
			output[i] = t - 'A' + 'a'
		} else {
			output[i] = t
		}
	}
	return output
}

// 二分法查找字元在子节点中的位置
// 如果查找成功，第一个返回参数为找到的位置，第二个返回参数为true
// 如果查找失败，第一个返回参数为应当插入的位置，第二个返回参数false
func binarySearch(nodes []*node, word Text) (int, bool) {
	start := 0
	end := len(nodes) - 1

	// 特例：
	if len(nodes) == 0 {
		// 当slice为空时，插入第一位置
		return 0, false
	}
	compareWithFirstWord := bytes.Compare(word, nodes[0].word)
	if compareWithFirstWord < 0 {
		// 当要查找的元素小于首元素时，插入第一位置
		return 0, false
	} else if compareWithFirstWord == 0 {
		// 当首元素等于node时
		return 0, true
	}
	compareWithLastWord := bytes.Compare(word, nodes[end].word)
	if compareWithLastWord == 0 {
		// 当尾元素等于node时
		return end, true
	} else if compareWithLastWord > 0 {
		// 当尾元素小于node时
		return end + 1, false
	}

	// 二分
	current := end / 2
	for end-start > 1 {
		compareWithCurrentWord := bytes.Compare(word, nodes[current].word)
		if compareWithCurrentWord == 0 {
			return current, true
		} else if compareWithCurrentWord < 0 {
			end = current
			current = (start + current) / 2
		} else {
			start = current
			current = (current + end) / 2
		}
	}
	return end, false
}

// 将字元加入节点数组中，并返回插入的节点指针
// 如果字元已经存在则返回存在的节点指针
func upsert(nodes *[]*node, word Text) *node {
	index, found := binarySearch(*nodes, word)
	if found {
		return (*nodes)[index]
	}
	*nodes = append(*nodes, nil)
	copy((*nodes)[index+1:], (*nodes)[index:])
	(*nodes)[index] = &node{word: word}
	return (*nodes)[index]
}

// 更新跳转信息:
// 	1. 当该位置从未被访问过时(jumper.minDistance为零的情况)，或者
//	2. 当该位置的当前最短路径大于新的最短路径时
// 将当前位置的最短路径值更新为baseDistance加上新分词的概率
func updateJumper(jumper *jumper, baseDistance float32, token *Token) {
	newDistance := baseDistance + token.distance
	if jumper.minDistance == 0 || jumper.minDistance > newDistance {
		jumper.minDistance = newDistance
		jumper.token = token
	}
}

// 取两整数较小值
func minInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// 取两整数较大值
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 输出分词结果为字符串
//
// 有两种输出模式，以"中华人民共和国"为例
//
//  普通模式（searchMode=false）输出一个分词"中华人民共和国/ns "
//  搜索模式（searchMode=true） 输出普通模式的再细致切分：
//      "中华/nz 人民/n 共和/nz 共和国/ns 人民共和国/nt 中华人民共和国/ns "
//
// 搜索模式主要用于给搜索引擎提供尽可能多的关键字，详情请见Token结构体的注释。
func SegmentsToString(segs []Segment, searchMode bool) (output string) {
	if searchMode {
		for _, seg := range segs {
			output += tokenToString(seg.token)
		}
	} else {
		for _, seg := range segs {
			output += fmt.Sprintf(
				"%s/%s ", textSliceToString(seg.token.text), seg.token.pos)
		}
	}
	return
}

func tokenToString(token *Token) (output string) {
	for _, s := range token.segments {
		output += tokenToString(s.token)
	}
	output += fmt.Sprintf("%s/%s ", textSliceToString(token.text), token.pos)
	return
}

// 输出分词结果到一个字符串slice
//
// 有两种输出模式，以"中华人民共和国"为例
//
//  普通模式（searchMode=false）输出一个分词"[中华人民共和国]"
//  搜索模式（searchMode=true） 输出普通模式的再细致切分：
//      "[中华 人民 共和 共和国 人民共和国 中华人民共和国]"
//
// 搜索模式主要用于给搜索引擎提供尽可能多的关键字，详情请见Token结构体的注释。

func SegmentsToSlice(segs []Segment, searchMode bool) (output []string) {
	if searchMode {
		for _, seg := range segs {
			output = append(output, tokenToSlice(seg.token)...)
		}
	} else {
		for _, seg := range segs {
			output = append(output, seg.token.Text())
		}
	}
	return
}

func tokenToSlice(token *Token) (output []string) {
	for _, s := range token.segments {
		output = append(output, tokenToSlice(s.token)...)
	}
	output = append(output, textSliceToString(token.text))
	return
}

// 将多个字元拼接一个字符串输出
func textSliceToString(text []Text) (output string) {
	for _, word := range text {
		output += string(word)
	}
	return
}

// 返回多个字元的字节总长度
func textSliceByteLength(text []Text) (length int) {
	for _, word := range text {
		length += len(word)
	}
	return
}
