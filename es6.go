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
    "github.com/zlyuancn/zerrors"
    "gopkg.in/olivere/elastic.v6"
)

type esv6Factory int

var _ IDBFactory = (*esv6Factory)(nil)

type ESv6Config struct {
    Address []string // 地址
}

func (esv6Factory) ParseTomlShard(shard *toml.Tree) (interface{}, error) {
    a := new(ESv6Config)
    if err := shard.Unmarshal(a); err != nil {
        return nil, err
    }
    return a, nil
}
func (esv6Factory) Connect(config interface{}) (interface{}, error) {
    conf, ok := config.(*ESv6Config)
    if !ok {
        return nil, zerrors.NewSimple("非*ESv6Config结构")
    }
    if conf == nil {
        return nil, zerrors.NewSimple("配置的值是空的")
    }

    c, err := elastic.NewClient(
        elastic.SetSniff(false),
        elastic.SetURL(conf.Address...),
    )
    if err != nil {
        return nil, zerrors.WrapSimple(err, "连接失败")
    }

    return c, nil
}
func (esv6Factory) Close(dbinstance interface{}) error {
    c, ok := dbinstance.(*elastic.Client)
    if !ok {
        return zerrors.NewSimple("非*elastic.Client结构")
    }

    c.Stop()
    return nil
}

// 获取esv6db实例
func GetESv6(dbname string) (*elastic.Client, error) {
    a := defaultDBFactory.GetDBInstance(dbname)
    if a == nil {
        return nil, zerrors.NewSimplef("不存在的dbname<%s>", dbname)
    }
    if a.Type() != DBESv6 {
        return nil, zerrors.NewSimplef("db实例<%s>是<%v>类型", dbname, a.dbtype)
    }

    return a.Instance().(*elastic.Client), nil
}

// 获取esv6db实例, 该实例如果不是esv6类型会panic
func MustGetESv6(dbname string) *elastic.Client {
    c, err := GetESv6(dbname)
    if err != nil {
        panic(err)
    }
    return c
}
