package pipeline

//基于"github.com/cznic/kv" 实现的pipline

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/aosen/search"
	"github.com/cznic/kv"
)

const (
	PersistentStorageFilePrefix = "db"
	StorageFolder               = "data"
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
func (self *KVPipline) Recover(shard int, internalIndexDocument func(docId uint64, data search.DocumentIndexData)) error {
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
		var data search.DocumentIndexData
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
