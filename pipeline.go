package search

//存储器
type SearchPipline interface {
	//初始化存储器, shard为初始化的集合编号
	Init()
	//获取存储集合数量, 集合数量可以提高并行计算效率
	GetStorageShards() int
	//连接数据库
	Conn(shard int)
	//关闭数据库连接
	Close(shard int)
	//将数据从shard DB恢复到内存
	Recover(shard int, internalIndexDocument func(docId uint64, data DocumentIndexData)) error
	//存储索引
	Set(shard int, key, value []byte)
	//从DB删除索引
	Delete(shard int, key []byte)
}
