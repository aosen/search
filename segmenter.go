package search

//分词接口，开发者只要实现分词接口，即可对接到引擎的分词模块

import (
	"bytes"
	"fmt"
	"unicode"
	"unicode/utf8"
)

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

// 字串类型，可以用来表达
//	1. 一个字元，比如"中"又如"国", 英文的一个字元是一个词
//	2. 一个分词，比如"中国"又如"人口"
//	3. 一段文字，比如"中国有十三亿人口"
type Text []byte

// 该结构体用于记录Viterbi算法中某字元处的向前分词跳转信息
type Jumper struct {
	MinDistance float32
	Token       *Token
}

// 文本中的一个分词
type Segment struct {
	// 分词在文本中的起始字节位置
	Start int
	// 分词在文本中的结束字节位置（不包括该位置）
	End int
	// 分词信息
	Token *Token
}

// 返回分词在文本中的起始字节位置
func (self *Segment) GetStart() int {
	return self.Start
}

// 返回分词在文本中的结束字节位置（不包括该位置）
func (self *Segment) GetEnd() int {
	return self.End
}

// 返回分词信息
func (self *Segment) GetToken() *Token {
	return self.Token
}

// 一个分词
type Token struct {
	// 分词的字串，这实际上是个字元数组
	TextList []Text
	// 分词在语料库中的词频
	Frequency int
	// log2(总词频/该分词词频)，这相当于log2(1/p(分词))，用作动态规划中
	// 该分词的路径长度。求解prod(p(分词))的最大值相当于求解
	// sum(distance(分词))的最小值，这就是“最短路径”的来历。
	Distance float32
	// 词性标注
	Pos string
	// 该分词文本的进一步分词划分，见Segments函数注释。
	Segments []*Segment
}

// 返回分词文本
func (token *Token) GetText() string {
	return TextSliceToString(token.TextList)
}

// 返回分词在语料库中的词频
func (token *Token) GetFrequency() int {
	return token.Frequency
}

// 返回分词词性标注
func (token *Token) GetPos() string {
	return token.Pos
}

// 该分词文本的进一步分词划分，比如"中华人民共和国中央人民政府"这个分词
// 有两个子分词"中华人民共和国"和"中央人民政府"。子分词也可以进一步有子分词
// 形成一个树结构，遍历这个树就可以得到该分词的所有细致分词划分，这主要
// 用于搜索引擎对一段文本进行全文搜索。
func (token *Token) GetSegments() []*Segment {
	return token.Segments
}

/*
Dictionary结构体实现了一个字串前缀树，
一个分词可能出现在叶子节点也有可能出现在非叶节点
*/
type Dictionary struct {
	Root           Node     // 根节点
	MaxTokenLength int      // 词典中最长的分词
	NumTokens      int      // 词典中分词数目
	Tokens         []*Token // 词典中所有的分词，方便遍历
	TotalFrequency int64    // 词典中所有分词的频率之和
}

// 前缀树节点
type Node struct {
	Word     Text    // 该节点对应的字元
	Token    *Token  // 当此节点没有对应的分词时值为nil
	Children []*Node // 该字元后继的所有可能字元，当为叶子节点时为空
}

// 词典中分词数目
func (self *Dictionary) GetNumTokens() int {
	return self.NumTokens
}

// 词典中所有分词的频率之和
func (self *Dictionary) GetTotalFrequency() int64 {
	return self.TotalFrequency
}

// 向词典中加入一个分词
func (self *Dictionary) AddToken(token *Token) {
	current := &self.Root
	for _, word := range token.TextList {
		// 一边向深处移动一边添加节点（如果需要的话）
		current = upsert(&current.Children, word)
	}

	// 当这个分词不存在词典中时添加此分词，否则忽略
	if current.Token == nil {
		current.Token = token
		if len(token.TextList) > self.MaxTokenLength {
			self.MaxTokenLength = len(token.TextList)
		}
		self.NumTokens++
		self.Tokens = append(self.Tokens, token)
		self.TotalFrequency += int64(token.Frequency)
	}
}

// 在词典中查找和字元组words可以前缀匹配的所有分词
// 返回值为找到的分词数
func (self *Dictionary) LookupTokens(words []Text, tokens []*Token) int {
	// 特殊情况
	if len(words) == 0 {
		return 0
	}

	current := &self.Root
	numTokens := 0
	for _, word := range words {
		// 如果已经抵达叶子节点则不再继续寻找
		if len(current.Children) == 0 {
			break
		}

		// 否则在该节点子节点中进行下个字元的匹配
		index, found := binarySearch(current.Children, word)
		if !found {
			break
		}

		// 匹配成功，则跳入匹配的子节点中
		current = current.Children[index]
		if current.Token != nil {
			tokens[numTokens] = current.Token
			numTokens++
		}
	}
	return numTokens
}

// 词典中最长的分词
func (self *Dictionary) GetMaxTokenLength() int {
	return self.MaxTokenLength
}

// 将文本划分成字元
func SplitTextToWords(text Text) []Text {
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
func binarySearch(nodes []*Node, word Text) (int, bool) {
	start := 0
	end := len(nodes) - 1

	// 特例：
	if len(nodes) == 0 {
		// 当slice为空时，插入第一位置
		return 0, false
	}
	compareWithFirstWord := bytes.Compare(word, nodes[0].Word)
	if compareWithFirstWord < 0 {
		// 当要查找的元素小于首元素时，插入第一位置
		return 0, false
	} else if compareWithFirstWord == 0 {
		// 当首元素等于node时
		return 0, true
	}
	compareWithLastWord := bytes.Compare(word, nodes[end].Word)
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
		compareWithCurrentWord := bytes.Compare(word, nodes[current].Word)
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
func upsert(nodes *[]*Node, word Text) *Node {
	index, found := binarySearch(*nodes, word)
	if found {
		return (*nodes)[index]
	}
	*nodes = append(*nodes, nil)
	copy((*nodes)[index+1:], (*nodes)[index:])
	(*nodes)[index] = &Node{Word: word}
	return (*nodes)[index]
}

// 更新跳转信息:
// 	1. 当该位置从未被访问过时(jumper.minDistance为零的情况)，或者
//	2. 当该位置的当前最短路径大于新的最短路径时
// 将当前位置的最短路径值更新为baseDistance加上新分词的概率
func UpdateJumper(jumper *Jumper, baseDistance float32, token *Token) {
	newDistance := baseDistance + token.Distance
	if jumper.MinDistance == 0 || jumper.MinDistance > newDistance {
		jumper.MinDistance = newDistance
		jumper.Token = token
	}
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
			output += TokenToString(seg.Token)
		}
	} else {
		for _, seg := range segs {
			output += fmt.Sprintf(
				"%s/%s ", TextSliceToString(seg.Token.TextList), seg.Token.Pos)
		}
	}
	return
}

func TokenToString(token *Token) (output string) {
	for _, s := range token.Segments {
		output += TokenToString(s.Token)
	}
	output += fmt.Sprintf("%s/%s ", TextSliceToString(token.TextList), token.Pos)
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
			output = append(output, TokenToSlice(seg.Token)...)
		}
	} else {
		for _, seg := range segs {
			output = append(output, seg.Token.GetText())
		}
	}
	return
}

func TokenToSlice(token *Token) (output []string) {
	for _, s := range token.Segments {
		output = append(output, TokenToSlice(s.Token)...)
	}
	output = append(output, TextSliceToString(token.TextList))
	return
}

// 将多个字元拼接一个字符串输出
func TextSliceToString(text []Text) (output string) {
	for _, word := range text {
		output += string(word)
	}
	return
}

// 返回多个字元的字节总长度
func TextSliceByteLength(text []Text) (length int) {
	for _, word := range text {
		length += len(word)
	}
	return
}
