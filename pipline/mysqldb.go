/*
Author: Aosen
QQ: 316052486
Data: 2016-01-14
Desc: 基于mysql的pipline实现
*/
package pipline

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"

	"github.com/aosen/search"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlPipline struct {
	//db列表
	dbs []*sql.DB
	//数据库连接信息
	dbinfo string
	//索引表的数量
	shardnum int
	//表的名称前缀
	tablePrefix string
}

type mysqlkeyvalue struct {
	Id    int
	Key   []byte
	Value []byte
}

func InitMysql(dbinfo string, shardnum int, tablePrefix string) *MysqlPipline {
	return &MysqlPipline{
		dbinfo:      dbinfo,
		shardnum:    shardnum,
		tablePrefix: tablePrefix,
	}
}

var CreateTable string = `
CREATE TABLE "?" (
    "id" integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
    "key" longblob NOT NULL,
    "value" longblob NOT NULL
);
`

//如果没有表就创建表
func (self *MysqlPipline) Init() {
	db, _ := sql.Open("mysql", self.dbinfo)
	db.SetMaxOpenConns(30)
	db.SetMaxIdleConns(30)
	err := db.Ping()
	if err != nil {
		panic("open mysql err: " + err.Error())
	}
	//初始化dbs列表，由于本身mysql db就是连接池属性，
	//所以只要Open一次就好
	for shard := 0; shard < self.shardnum; shard++ {
		self.dbs[shard] = db
	}
	//如果mysql中不存在索引表，则创建
	stmt, err := db.Prepare(CreateTable)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer stmt.Close()
	for shard := 0; shard < self.shardnum; shard++ {
		if result, err := stmt.Exec(self.tablePrefix + strconv.Itoa(shard)); err == nil {
			log.Println(result)
		}
	}
}

func (self *MysqlPipline) GetStorageShards() int {
	return self.shardnum
}

//连接数据库
func (self *MysqlPipline) Conn(shard int) {
}

//关闭数据库连接
func (self *MysqlPipline) Close(shard int) {
	self.dbs[shard].Close()
}

//数据恢复
func (self *MysqlPipline) Recover(shard int, internalIndexDocument func(docId uint64, data search.DocumentIndexData)) error {
	return nil
}

//数据存储
func (self *MysqlPipline) Set(shard int, key, value []byte) {
}

//数据删除
func (self *MysqlPipline) Delete(shard int, key []byte) {
}
