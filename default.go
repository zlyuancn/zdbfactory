/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/5
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "github.com/pelletier/go-toml"
)

var defaultDBFactory = New(WithAutoCloseDB())

// 添加toml文件, 重复的db名会被替换掉
func AddTomlFile(file string) error {
    return defaultDBFactory.AddTomlFile(file)
}

// 添加toml树, 重复的db名会被替换掉
func AddTomlTree(tree *toml.Tree) error {
    return defaultDBFactory.AddTomlTree(tree)
}

// 添加toml分片, 重复的db名会被替换掉
func AddTomlShard(dbname string, shard *toml.Tree) error {
    return defaultDBFactory.AddTomlShard(dbname, shard)
}

// 添加db配置, 重复的db名会被替换掉
func AddDBConfig(dbname string, dbtype DBType, config interface{}) {
    defaultDBFactory.AddDBConfig(dbname, dbtype, config)
}

// 移除db, 移除之前会关闭连接
func RemoveDB(dbname string) {
    defaultDBFactory.RemoveDB(dbname)
}

// 连接所有db
func ConnectAllDB() error {
    return defaultDBFactory.ConnectAllDB()
}

// 关闭所有db连接
func CloseAllDb() {
    defaultDBFactory.CloseAllDb()
}
