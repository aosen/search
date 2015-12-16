package search

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/cznic/kv"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"os"
	"strconv"
)

const (
	NumNanosecondsInAMillisecond = 1000000
	PersistentStorageFilePrefix  = "db"
	StorageFolder                = "data"
)

type KVPipline struct {
	dbs []*kv.DB
	//数据库集合个数
	shardnum int
	//存储的文件目录
	storageFolder string
}

func InitKV(shard int) *KVPipline {
	return &KVPipline{
		storageFolder: StorageFolder,
		shardnum:      shard,
	}
}

func (self *KVPipline) GetStorageShards() int {
	return self.shardnum
}

func (self *KVPipline) Init() {
	err := os.MkdirAll(self.storageFolder, 0700)
	if err != nil {
		log.Fatal("无法创建目录", self.storageFolder)
	}

	// 打开或者创建数据库
	self.dbs = make([]*kv.DB, self.shardnum)
	for shard := 0; shard < self.shardnum; shard++ {
		dbPath := self.storageFolder + "/" + "db." + strconv.Itoa(shard)
		db, err := OpenOrCreateKv(dbPath, &kv.Options{})
		if db == nil || err != nil {
			log.Fatal("无法打开数据库", dbPath, ": ", err, db)
		}
		self.dbs[shard] = db
	}
}

//连接数据库
func (self *KVPipline) Conn(shard int) {
	dbPath := self.storageFolder + "/" + "db." + strconv.Itoa(shard)
	db, err := OpenOrCreateKv(dbPath, &kv.Options{})
	if db == nil || err != nil {
		log.Fatal("无法打开数据库", dbPath, ": ", err, db)
	}
	self.dbs[shard] = db
}

//关闭数据连接
func (self *KVPipline) Close(shard int) {
	self.dbs[shard].Close()
}

//从shard 恢复数据
func (self *KVPipline) Recover(shard int, internalIndexDocument func(docId uint64, data DocumentIndexData)) error {
	iter, err := self.dbs[shard].SeekFirst()
	if err != nil {
		return err
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
		internalIndexDocument(docId, data)
	}
	return nil
}

//将key－value存储到哪个集合中
func (self *KVPipline) Set(shard int, key, value []byte) {
	self.dbs[shard].Set(key, value)
}

func (self *KVPipline) Delete(shard int, key []byte) {
	self.dbs[shard].Delete(key)
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

type KeyValue struct {
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
	for shard := 0; shard < self.shardnum; shard++ {
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

func (self *MongoPipline) Recover(shard int, internalIndexDocument func(docId uint64, data DocumentIndexData)) error {
	c := self.sessions[shard].DB(self.mongoDBName).C(self.collectionPrefix + strconv.Itoa(shard))
	var keyvalues []KeyValue
	err := c.Find(nil).All(&keyvalues)
	if err != nil {
		return err
	}
	for _, kv := range keyvalues {
		// 得到docID
		docId, _ := binary.Uvarint(kv.Key)
		// 得到data
		buf := bytes.NewReader(kv.Value)
		dec := gob.NewDecoder(buf)
		var data DocumentIndexData
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
	err := c.Insert(&KeyValue{
		Id_:   bson.NewObjectId(),
		Key:   key,
		Value: value,
	})
	if err != nil {
		log.Println("store kv err: " + err.Error())
	}
}

func (self *MongoPipline) Delete(shard int, key []byte) {
	c := self.sessions[shard].DB(self.mongoDBName).C(self.collectionPrefix + strconv.Itoa(shard))
	c.Remove(bson.M{"key": key})
}
