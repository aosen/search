/*
Author: Aosen
QQ: 316052486
Data: 2016-01-14
Desc: 基于mysql的pipline实现, 本实例仅供参考，还需根据实际开发需求进行改装
*/
package pipeline

import (
	"fmt"
	"strconv"

	"github.com/aosen/search"
	"github.com/astaxie/beego/orm"
)

type MysqlPipline struct {
	//数据库连接信息
	dbinfo string
	//索引表的数量
	shardnum int
	//表的名称前缀
	tablePrefix string
}

type mysqlkeyvalue struct {
	Id    int
	Key   string `orm:"size(100);unique"`
	Value string `orm:"type(text)"`
}

func InitMysql(dbinfo string, shardnum int, tablePrefix string) *MysqlPipline {
	return &MysqlPipline{
		dbinfo:      dbinfo,
		shardnum:    shardnum,
		tablePrefix: tablePrefix,
	}
}

var CreateTable string = `CREATE TABLE %s (
    id integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
    k varchar(100) NOT NULL,
    v longtext NOT NULL
)
;`

//如果没有表就创建表
func (self *MysqlPipline) Init() {
	orm.RegisterDriver("mysql", orm.DR_MySQL)
	orm.RegisterDataBase("search", "mysql", self.dbinfo)
	orm.RegisterModel(new(mysqlkeyvalue))
	//create table
	//orm.RunSyncdb("default", false, true)
	//如果没有索引数据表，则创建
	o := orm.NewOrm()
	o.Using("search") // 默认使用 default，你可以指定为其他数据库
	//如果表不存在则创建表
	for i := 0; i < self.shardnum; i++ {
		o.Raw(fmt.Sprintf(CreateTable, self.tablePrefix+strconv.Itoa(i))).Exec()
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
