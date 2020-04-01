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

    "github.com/zlyuancn/zerrors"
    "github.com/zlyuancn/zmongo"
)

type mongoFactory int

var _ IDBFactory = (*mongoFactory)(nil)

type MongoConfig struct {
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

func (mongoFactory) MakeEmptyConfig() interface{} {
    return new(MongoConfig)
}

func (mongoFactory) Connect(config interface{}) (interface{}, error) {
    var conf *MongoConfig
    switch c := config.(type) {
    case *MongoConfig:
        conf = c
    case MongoConfig:
        conf = &c
    default:
        return nil, zerrors.NewSimple("非*MongoConfig结构")
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

// 添加mongo配置
func AddMongoConfig(dbname string, conf *MongoConfig) {
    AddDBConfig(dbname, Mongo, conf)
}

// 获取mongodb实例
func GetMongo(dbname string) (*zmongo.Client, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != Mongo {
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
