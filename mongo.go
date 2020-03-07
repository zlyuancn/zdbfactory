/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/4
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "time"

    "github.com/pelletier/go-toml"
    "github.com/zlyuancn/zerrors"
    "github.com/zlyuancn/zmongo"
)

type mongoFactory int

var _ IDBFactory = (*mongoFactory)(nil)

type MongoDBConfig struct {
    Address       []string // 连接地址, 如: 127.0.0.1:27017
    DBName        string   // 库名
    UserName      string   // 用户名
    Password      string   // 密码
    PoolSize      uint64   // 连接池的数量
    DialTimeout   int64    // 连接超时(毫秒
    DoTimeout     int64    // 操作超时(毫秒
    SocketTimeout int64    // Socket超时
    Ping          bool     // 开始连接时是否ping确认连接情况
}

func (mongoFactory) ParseTomlShard(shard *toml.Tree) (interface{}, error) {
    a := new(MongoDBConfig)
    if err := shard.Unmarshal(a); err != nil {
        return nil, err
    }
    return a, nil
}
func (mongoFactory) Connect(config interface{}) (interface{}, error) {
    conf, ok := config.(*MongoDBConfig)
    if !ok {
        return nil, zerrors.NewSimple("非*MongoDBConfig结构")
    }
    if conf == nil {
        return nil, zerrors.NewSimple("配置的值是空的")
    }

    c, err := zmongo.New(&zmongo.Config{
        Address:       conf.Address,
        DBName:        conf.DBName,
        UserName:      conf.UserName,
        Password:      conf.Password,
        PoolSize:      conf.PoolSize,
        DialTimeout:   time.Duration(conf.DialTimeout * 1e6),
        DoTimeout:     time.Duration(conf.DoTimeout * 1e6),
        SocketTimeout: time.Duration(conf.SocketTimeout * 1e6),
    })
    if err != nil {
        return nil, zerrors.WrapSimple(err, "连接失败")
    }

    if conf.Ping {
        if err = c.Ping(nil); err != nil {
            return nil, zerrors.WrapSimple(err, "ping失败")
        }
    }

    return c, nil
}
func (mongoFactory) Close(dbinstance interface{}) error {
    c, ok := dbinstance.(*zmongo.Client)
    if !ok {
        return zerrors.NewSimple("非*zmongo.Client结构")
    }

    return c.Close()
}

// 获取mongodb实例
func GetMongo(dbname string) (*zmongo.Client, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != DBMongoDB {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.dbtype)
    }

    return a.Instance().(*zmongo.Client), nil
}

// 获取mongodb实例, 该实例如果不是mongo类型会panic
func MustGetMongo(dbname string) *zmongo.Client {
    c, err := GetMongo(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
