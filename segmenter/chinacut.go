package segmenter

//中文分词的基类
//Segmenter

import (
	"bufio"
	"fmt"
	"github.com/aosen/search"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

//分词器接口
const (
	MinTokenFrequency = 2 // 仅从字典文件中读取大于等于此频率的分词
)

//分词器结构体
type ChinaCut struct {
	dict *search.Dictionary
}

func InitChinaCut(files string) *ChinaCut {
	seg := &ChinaCut{}
	seg.LoadDictionary(files)
	return seg
}

// 返回分词器使用的词典
func (self *ChinaCut) Dictionary() *search.Dictionary {
	return self.dict
}

func (self *ChinaCut) segmentWords(text []search.Text, searchMode bool) []search.Segment {
	// 搜索模式下该分词已无继续划分可能的情况
	if searchMode && len(text) == 1 {
		return []search.Segment{}
	}

	// jumpers定义了每个字元处的向前跳转信息，包括这个跳转对应的分词，
	// 以及从文本段开始到该字元的最短路径值
	jumpers := make([]search.Jumper, len(text))

	tokens := make([]*search.Token, self.dict.MaxTokenLength)
	for current := 0; current < len(text); current++ {
		// 找到前一个字元处的最短路径，以便计算后续路径值
		var baseDistance float32
		if current == 0 {
			// 当本字元在文本首部时，基础距离应该是零
			baseDistance = 0
		} else {
			baseDistance = jumpers[current-1].MinDistance
		}

		// 寻找所有以当前字元开头的分词
		numTokens := self.dict.LookupTokens(
			text[current:minInt(current+self.dict.MaxTokenLength, len(text))], tokens)

		// 对所有可能的分词，更新分词结束字元处的跳转信息
		for iToken := 0; iToken < numTokens; iToken++ {
			location := current + len(tokens[iToken].TextList) - 1
			if !searchMode || current != 0 || location != len(text)-1 {
				search.UpdateJumper(&jumpers[location], baseDistance, tokens[iToken])
			}
		}

		// 当前字元没有对应分词时补加一个伪分词
		if numTokens == 0 || len(tokens[0].GetText()) > 1 {
			search.UpdateJumper(&jumpers[current], baseDistance,
				&search.Token{TextList: []search.Text{text[current]}, Frequency: 1, Distance: 32, Pos: "x"})
		}
	}

	// 从后向前扫描第一遍得到需要添加的分词数目
	numSeg := 0
	for index := len(text) - 1; index >= 0; {
		location := index - len(jumpers[index].Token.TextList) + 1
		numSeg++
		index = location - 1
	}

	// 从后向前扫描第二遍添加分词到最终结果
	outputSegments := make([]search.Segment, numSeg)
	for index := len(text) - 1; index >= 0; {
		location := index - len(jumpers[index].Token.TextList) + 1
		numSeg--
		outputSegments[numSeg].Token = jumpers[index].Token
		index = location - 1
	}

	// 计算各个分词的字节位置
	bytePosition := 0
	for iSeg := 0; iSeg < len(outputSegments); iSeg++ {
		outputSegments[iSeg].Start = bytePosition
		bytePosition += search.TextSliceByteLength(outputSegments[iSeg].Token.TextList)
		outputSegments[iSeg].End = bytePosition
	}
	return outputSegments
}

// 从文件中载入词典
// 可以载入多个词典文件，文件名用","分隔，排在前面的词典优先载入分词，比如
// 	"用户词典.txt,通用词典.txt"
// 当一个分词既出现在用户词典也出现在通用词典中，则优先使用用户词典。
// 词典的格式为（每个分词一行）：
//	分词文本 频率 词性
func (self *ChinaCut) LoadDictionary(files string) {
	self.dict = new(search.Dictionary)
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
			if frequency < MinTokenFrequency {
				continue
			}

			// 将分词添加到字典中
			words := search.SplitTextToWords([]byte(text))
			token := search.Token{TextList: words, Frequency: frequency, Pos: pos}
			self.dict.AddToken(&token)
		}
	}

	// 计算每个分词的路径值，路径值含义见Token结构体的注释
	logTotalFrequency := float32(math.Log2(float64(self.dict.TotalFrequency)))
	for _, token := range self.dict.Tokens {
		token.Distance = logTotalFrequency - float32(math.Log2(float64(token.Frequency)))
	}

	// 对每个分词进行细致划分，用于搜索引擎模式，该模式用法见Token结构体的注释。
	for _, token := range self.dict.Tokens {
		segments := self.segmentWords(token.TextList, true)

		// 计算需要添加的子分词数目
		numTokensToAdd := 0
		for iToken := 0; iToken < len(segments); iToken++ {
			if len(segments[iToken].Token.TextList) > 1 {
				// 略去字元长度为一的分词
				// TODO: 这值得进一步推敲，特别是当字典中有英文复合词的时候
				numTokensToAdd++
			}
		}
		token.Segments = make([]*search.Segment, numTokensToAdd)

		// 添加子分词
		iSegmentsToAdd := 0
		for iToken := 0; iToken < len(segments); iToken++ {
			if len(segments[iToken].Token.TextList) > 1 {
				token.Segments[iSegmentsToAdd] = &segments[iToken]
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
func (self *ChinaCut) Cut(bytes []byte, model bool) []search.Segment {
	// 处理特殊情况
	if len(bytes) == 0 {
		return []search.Segment{}
	}
	// 划分字元
	text := search.SplitTextToWords(bytes)
	return self.segmentWords(text, model)
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
