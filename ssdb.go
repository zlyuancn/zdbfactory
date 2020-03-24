/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/7
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "github.com/pelletier/go-toml"
    "github.com/seefan/gossdb"
    ssdbconf "github.com/seefan/gossdb/conf"
    "github.com/zlyuancn/zerrors"
)

type ssdbFactory int

var _ IDBFactory = (*ssdbFactory)(nil)

type SsdbConfig struct {
    Host             string
    Port             int
    Password         string
    GetClientTimeout int  // 获取客户端超时(毫秒)
    MinPoolSize      int  // 最小连接池数
    MaxPoolSize      int  // 最大连接池个数
    RetryEnabled     bool // 是否启用重试，设置为true时，如果请求失败会再重试一次
}

func (ssdbFactory) ParseTomlShard(shard *toml.Tree) (interface{}, error) {
    a := new(SsdbConfig)
    if err := shard.Unmarshal(a); err != nil {
        return nil, err
    }
    return a, nil
}
func (ssdbFactory) Connect(config interface{}) (interface{}, error) {
    var conf *SsdbConfig
    switch c := config.(type) {
    case *SsdbConfig:
        conf = c
    case SsdbConfig:
        conf = &c
    default:
        return nil, zerrors.NewSimple("非*SsdbConfig结构")
    }

    pool, err := gossdb.NewPool(&ssdbconf.Config{
        Host:             conf.Host,
        Port:             conf.Port,
        Password:         conf.Password,
        GetClientTimeout: conf.GetClientTimeout / 1e3,
        MinPoolSize:      conf.MinPoolSize,
        MaxPoolSize:      conf.MaxPoolSize,
        RetryEnabled:     conf.RetryEnabled,
    })
    if err != nil {
        return nil, zerrors.WrapSimple(err, "连接失败")
    }

    return pool, nil
}
func (ssdbFactory) Close(dbinstance interface{}) error {
    c, ok := dbinstance.(*gossdb.Connectors)
    if !ok {
        return zerrors.NewSimple("非*gossdb.Connectors结构")
    }

    c.Close()
    return nil
}

// 添加ssdb配置
func AddSsdbConfig(dbname string, conf *SsdbConfig) {
    AddDBConfig(dbname, SSDB, conf)
}

// 获取ssdb实例
func GetSsdb(dbname string) (*gossdb.Connectors, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != SSDB {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.dbtype)
    }

    return a.Instance().(*gossdb.Connectors), nil
}

// 获取ssdb实例, 该实例如果不是ssdb类型会panic
func MustGetSsdb(dbname string) *gossdb.Connectors {
    c, err := GetSsdb(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
