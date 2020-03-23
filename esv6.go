/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/3/7
   Description :
-------------------------------------------------
*/

package zdbfactory

import (
    "context"
    "time"

    "github.com/pelletier/go-toml"
    "github.com/zlyuancn/zerrors"
    "gopkg.in/olivere/elastic.v6"
)

type esv6Factory int

var _ IDBFactory = (*esv6Factory)(nil)

type ESv6Config struct {
    Address       []string // 地址
    UserName      string   // 用户名
    Password      string   // 密码
    DialTimeout   int64    // 连接超时(毫秒
    Sniff         bool     // 嗅探器
    Healthcheck   bool     // 心跳检查
    Retry         int      // 重试次数
    RetryInterval int      // 重试间隔(毫秒)
    GZip          bool     // 启用gzip压缩
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

    opts := []elastic.ClientOptionFunc{
        elastic.SetSniff(conf.Sniff),
        elastic.SetURL(conf.Address...),
        elastic.SetHealthcheck(conf.Healthcheck),
        elastic.SetGzip(conf.GZip),
    }
    if conf.UserName != "" || conf.Password != "" {
        opts = append(opts, elastic.SetBasicAuth(conf.UserName, conf.Password))
    }
    if conf.Retry > 0 {
        ticks := make([]int, conf.Retry)
        for i := 0; i < conf.Retry; i++ {
            ticks[i] = conf.RetryInterval
        }
        elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewSimpleBackoff(ticks...)))
    }

    ctx := context.Background()
    if conf.DialTimeout > 0 {
        ctx, _ = context.WithTimeout(ctx, time.Duration(conf.DialTimeout*1e6))
    }

    c, err := elastic.DialContext(ctx, opts...)
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
    if a.Type() != ESv6 {
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
