package pipeline

//基于mongodb实现的pipline

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"strconv"

	"github.com/aosen/search"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoPipline struct {
	sessions []*mgo.Session
	url      string
	//数据库名
	mongoDBName string
	//数据库集合个数
	shardnum int
	//集合名称前缀
	collectionPrefix string
}

type mgokeyvalue struct {
	Id_   bson.ObjectId `bson:"_id"`
	Key   []byte        `bson:"key"`
	Value []byte        `bson:"Value"`
}

func InitMongo(db string, shardnum int, url string, collectionPrefix string) *MongoPipline {
	return &MongoPipline{
		mongoDBName:      db,
		shardnum:         shardnum,
		url:              url,
		collectionPrefix: collectionPrefix,
	}
}

func (self *MongoPipline) Init() {
	self.sessions = make([]*mgo.Session, self.shardnum)
	session, err := mgo.Dial(self.url)
	if err != nil {
		panic("open mongodb file:" + err.Error())
	}
	if err = session.Ping(); err != nil {
		panic("MongoDB execute ping error:" + err.Error())
	}
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	for shard := 0; shard < self.shardnum; shard++ {
		self.sessions[shard] = session
	}
}

func (self *MongoPipline) GetStorageShards() int {
	return self.shardnum
}

//连接数据库
func (self *MongoPipline) Conn(shard int) {
	session, err := mgo.Dial(self.url)
	if err != nil {
		panic("open mongodb file:" + err.Error())
	}
	if err = session.Ping(); err != nil {
		panic("MongoDB execute ping error:" + err.Error())
	}
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	self.sessions[shard] = session
}

//关闭数据库连接
func (self *MongoPipline) Close(shard int) {
	self.sessions[shard].Close()
}

func (self *MongoPipline) Recover(shard int, internalIndexDocument func(docId uint64, data search.DocumentIndexData)) error {
	c := self.sessions[shard].DB(self.mongoDBName).C(self.collectionPrefix + strconv.Itoa(shard))
	var mgokeyvalues []mgokeyvalue
	err := c.Find(nil).All(&mgokeyvalues)
	if err != nil {
		return err
	}
	for _, kv := range mgokeyvalues {
		// 得到docID
		docId, _ := binary.Uvarint(kv.Key)
		// 得到data
		buf := bytes.NewReader(kv.Value)
		dec := gob.NewDecoder(buf)
		var data search.DocumentIndexData
		err := dec.Decode(&data)
		if err != nil {
			continue
		}
		// 添加索引
		internalIndexDocument(docId, data)
	}
	return nil
}

//将key－value存储到哪个集合中
func (self *MongoPipline) Set(shard int, key, value []byte) {
	c := self.sessions[shard].DB(self.mongoDBName).C(self.collectionPrefix + strconv.Itoa(shard))
	err := c.Insert(&mgokeyvalue{
		Id_:   bson.NewObjectId(),
		Key:   key,
		Value: value,
	})
	if err != nil {
		panic("store kv err: " + err.Error())
	}
}

func (self *MongoPipline) Delete(shard int, key []byte) {
	c := self.sessions[shard].DB(self.mongoDBName).C(self.collectionPrefix + strconv.Itoa(shard))
	c.Remove(bson.M{"key": key})
}
